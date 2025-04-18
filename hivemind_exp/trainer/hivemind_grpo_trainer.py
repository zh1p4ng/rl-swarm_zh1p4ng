import gc
import hashlib
import logging
import time
import traceback
from typing import Any

import datasets
import torch
from hivemind.dht import DHT
from hivemind.utils import get_dht_time
from trl import GRPOConfig, GRPOTrainer

from hivemind_exp.debug_utils import print_system_info
from hivemind_exp.dht_utils import (
    ROUND_STAGE_NUMBER_KEY,
    get_dht_value,
    get_round_and_stage,
    leaderboard_key,
    node_outputs_key,
    rewards_key,
)
from hivemind_exp.hivemind_utils import HivemindNode, StageData
from hivemind_exp.name_utils import get_name_from_peer_id


class HivemindGRPOTrainer:
    """
    Subclass of GRPOTrainer that implements multi-stage GRPO by publishing
    intermediate results to a connected Hivemind DHT.
    """

    class PublishingGRPOTrainer(GRPOTrainer):
        def __init__(
                self,
                node: HivemindNode,
                dht: DHT,
                tokenizer,
                logger,
                **kwargs,
        ):
            self.node = node
            self.dht = dht
            self.logger = logger
            self.stage_rewards = 0.0
            super().__init__(processing_class=tokenizer, **kwargs)

        def publish_leaderboard(self):
            r, s = self.node.round_num, self.node.stage_num
            curr_rewards: dict[str, Any] | None = get_dht_value(
                self.dht, key=rewards_key(r, s), latest=True
            )
            if curr_rewards:
                # Sorted list of (node_key, reward) pairs.
                leaderboard = list(
                    sorted(
                        curr_rewards.items(), key=lambda t: (t[1], t[0]), reverse=True
                    )
                )
                self.dht.store(
                    key=leaderboard_key(r, s),
                    value=leaderboard,
                    expiration_time=get_dht_time() + self.node.out_expiration,
                )
            else:
                self.logger.info(f"Can't retrieve round {r} stage {s - 1} rewards")

        def compute_loss(self, model, inputs, *args, **kwargs):
            loss = super().compute_loss(model, inputs, *args, **kwargs)
            # 奖励函数必须保存 node.outputs + node.rewards！
            # 这只是为了在正确的时间发布到 DHT。
            question = self.node.outputs["question"]
            value = (time.time(), self.node.outputs)
            self.dht.store(
                key=node_outputs_key(self.node),
                subkey=question,
                value=value,
                expiration_time=get_dht_time() + self.node.out_expiration,
            )
            self.node.put_stage_outputs(
                self.node.round_num, self.node.stage_num, question, value
            )

            # Just the latest.
            self.stage_rewards += sum(self.node.rewards)
            self.dht.store(
                key=rewards_key(self.node.round_num, self.node.stage_num),
                subkey=self.node.key,
                value=self.stage_rewards,
                expiration_time=get_dht_time() + self.node.out_expiration,
            )
            if self.node.is_coordinator:
                self.publish_leaderboard()

            return loss

    def __init__(
            self,
            node: HivemindNode,
            dht: DHT,
            stage_data: StageData,
            config: GRPOConfig,
            model,
            tokenizer,
            log_tag=None,
            **kwargs,
    ):
        self.node = node
        self.dht = dht

        self.stage_data = stage_data

        self.config = config
        assert self.config.output_dir
        self.config.output_dir += f"-{get_name_from_peer_id(self.node.key, True)}"
        self.model = model
        self.tokenizer = tokenizer
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token

        if not log_tag:
            log_tag = self.node.key

        self.logger = logging.getLogger(f"{__name__}: {log_tag}")

    def wait_for(self, result_fn=lambda: None, interval=10, timeout=30):
        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout:
            result = result_fn()
            if result is None:
                time.sleep(interval)
            else:
                break
        return result

    def train_stages(self, round_num, start_stage, is_coordinator):
        self.node.round_num = round_num
        for i, stage in enumerate(self.stage_data.stages[start_stage:]):
            stage_num = start_stage + i
            self.node.stage_num = stage_num

            if is_coordinator:
                self.dht.store(
                    key=ROUND_STAGE_NUMBER_KEY,
                    value=(self.node.round_num, stage_num),
                    expiration_time=get_dht_time() + self.node.out_expiration,
                )

            self.logger.info(f"📈 Training round: {round_num} stage: {stage_num}")
            train_dataset, test_dataset = stage.datasets_fn(round_num, stage_num)
            kwargs = {
                "model": self.model,
                "args": self.config,
                "reward_funcs": stage.reward_funcs,
                "train_dataset": train_dataset,
                "eval_dataset": test_dataset,
            }
            trainer = HivemindGRPOTrainer.PublishingGRPOTrainer(
                self.node, self.dht, self.tokenizer, self.logger, **kwargs
            )
            self.train_and_save(trainer, train_dataset)
            self.logger.info(
                f"📉 Finished training round: {round_num} stage: {stage_num}"
            )

        if self.config.push_to_hub_token is not None:
            self.logger.info("Pushing model to Hugging Face Hub...")
            try:
                trainer.push_to_hub(
                    tags=[
                        "rl - swarm",
                        "grpo",
                        "gensyn",
                        f"I am {get_name_from_peer_id(self.node.key)}",
                    ]
                )
                time.sleep(1)
            except Exception:
                self.logger.info(
                    "Failed to push model to the Hugging Face Hub. When you conclude training please try manually pushing it yourself using the instructions here: https://huggingface.co/docs/hub/en/models-uploading"
                )

        self.cleanup()

    def cleanup(self):
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        torch.cuda.ipc_collect()
        if torch.backends.mps.is_available():  # type: ignore
            torch.mps.empty_cache()  # type: ignore
        try:
            if torch.xpu.is_available():  # type: ignore
                torch.xpu.empty_cache()  # type: ignore
        except AttributeError:
            pass
        self.node.clear_stage_cache()

    def train_and_save(self, trainer, train_dataset):
        for num_fails in range(MAX_TRAIN_FAILS):
            try:
                train_result = trainer.train()
                break
            except (BlockingIOError, EOFError) as e:
                self.logger.warning(f"DHT IPC error: {e}. Restarting training...")
                self.cleanup()  # Clear GPU/caches
                time.sleep(5)
                continue
        metrics = train_result.metrics
        metrics["train_samples"] = len(train_dataset)
        trainer.log_metrics("train", metrics)
        trainer.save_metrics("train", metrics)
        trainer.save_state()

        self.logger.info("Saving model")
        trainer.model.config.use_cache = True
        trainer.save_model(self.config.output_dir)
        self.logger.info(f"Tokenizer saved to {self.config.output_dir}")
        assert self.config.distributed_state
        self.config.distributed_state.wait_for_everyone()

        self.tokenizer.save_pretrained(self.config.output_dir)
        self.logger.info(f"Tokenizer saved to {self.config.output_dir}")

    def get_round_and_stage(self):
        return get_round_and_stage(self.dht)

    def coordinator_train(self):
        round_num = 0
        start_time = time.monotonic()
        while (
                round_num < self.stage_data.max_rounds
                and time.monotonic() - start_time < self.stage_data.train_timeout
        ):
            self.logger.info(f"🤖 Starting new round: {round_num}")

            _ = self.dht.get_visible_maddrs(latest=True)
            self.train_stages(round_num, 0, is_coordinator=True)

            round_num += 1
            if round_num == self.stage_data.max_rounds:
                return

        self.logger.info("Training timed out!")

    def follower_train(
            self, check_interval=5.0, log_timeout=10.0, max_check_interval=60.0 * 5
    ):
        done_rounds = set()
        start_time = time.monotonic()
        fetch_log_time = start_time
        check_backoff = check_interval
        while time.monotonic() - start_time < self.stage_data.train_timeout:
            curr_time = time.monotonic()
            _ = self.dht.get_visible_maddrs(latest=True)

            try:
                round_num, stage = self.get_round_and_stage()
            except Exception as e:
                if curr_time - fetch_log_time > log_timeout:
                    self.logger.debug(
                        f"Could not fetch round and stage: {e}. Next check in {check_interval}s."
                    )
                    fetch_log_time = curr_time

                time.sleep(check_interval)
                continue

            if round_num not in done_rounds:
                self.logger.info(
                    f"🐝 Joining round: {round_num} starting at stage: {stage}"
                )
                try:
                    self.train_stages(round_num, stage, is_coordinator=False)
                except datasets.exceptions.DatasetGenerationError:
                    if stage > 0:
                        self.logger.info("Re-attempting training starting at stage 0!")
                        self.train_stages(round_num, 0, is_coordinator=False)
                    else:
                        raise

                done_rounds.add(round_num)
                check_backoff = check_interval
            else:
                self.logger.info(
                    f"Already finished round: {round_num}. Next check in {check_backoff}s."
                )
                time.sleep(check_backoff)
                check_backoff = min(check_backoff * 2, max_check_interval)

            if round_num == self.stage_data.max_rounds - 1:
                return

        self.logger.info("Training timed out!")

    def catch_up_train(self, start_round=0, end_round=None):
        """
        补跑从 start_round 到 end_round的轮次。
        如果 end_round为None, 则补跑至当前轮次的前一轮。
        """
        curr_round, _ = self.get_round_and_stage()
        if end_round is None:
            end_round = curr_round - 1

            self.logger.info(f"开始补跑轮次, 从{start_round}到{end_round}")
            done_rounds = set()

            for round_num in range(start_round, end_round + 1):
                if round_num in done_rounds:
                    self.logger.info(f"轮次 {round_num} 已完成, 跳过")
                    continue

                self.logger.info(f"补跑轮次: {round_num}, 从  stage 0 开始")
                try:
                    self.train_stages(round_num, 0, is_coordinator=False)
                    done_rounds.add(round_num)
                    self.cleanup()
                except datasets.exceptions.DatasetGenerationError as e:
                    self.logger.error(f"轮次 {round_num}  数据生成失败: {e}")
                    continue
                except Exception as e:
                    self.logger.error(f"轮次 {round_num} 训练失败: {e}")
                    break

            self.logger.info(f"补跑完成, 从 {start_round} 到 {end_round}")

    def train(self):
        try:
            if self.node.is_coordinator:
                self.coordinator_train()
            else:
                curr_round, _ = self.get_round_and_stage()
                if curr_round > 0:
                    self.catch_up_train(start_round=0, end_round=curr_round - 1)
                    self.follower_train()
        except Exception:
            import traceback
            traceback.print_exc()
