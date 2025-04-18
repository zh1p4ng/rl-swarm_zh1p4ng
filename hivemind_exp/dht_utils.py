import hashlib
from functools import lru_cache
from typing import Any

from hivemind.dht import DHT
from hivemind.utils import ValueWithExpiration

from hivemind_exp.hivemind_utils import HivemindNode

ROUND_STAGE_NUMBER_KEY = "rl_swarm_rs"  # No subkeys. Coordinator publishes.

# Round and stage (e.g. 0_0) appended.
LEADERBOARD_KEY_PREFIX = (
    "rl_swarm_leaderboard"  # Subkey = Metric. Coordinator publishes.
)
REWARDS_KEY = "rl_swarm_rewards"  # Subkey = Metric. Everyone publishes.

# Node key, round, and stage (e.g. abcde_0_0) appended.
OUTPUTS_KEY_PREFIX = "rl_swarm_outputs"  # Subkey = Example Hash. Everyone publishes.


def leaderboard_key(round_num, stage) -> str:
    return f"{LEADERBOARD_KEY_PREFIX}_{round_num}_{stage}"


def rewards_key(round_num, stage) -> str:
    return f"{REWARDS_KEY}_{round_num}_{stage}"


def outputs_key(node_key: str, round_num, stage) -> str:
    return f"{OUTPUTS_KEY_PREFIX}_{node_key}_{round_num}_{stage}"


def node_outputs_key(node: HivemindNode) -> str:
    return outputs_key(node.key, node.round_num, node.stage_num)


def hash_keys(outputs):
    # Handles older versions of the trainer that did not hash question keys.
    result = {}
    for k, v in outputs.items():
        if len(k) != 32:  # Not perfect, but good enough.
            k = hashlib.md5(k.encode()).hexdigest()
        result[k] = v

    return result


@lru_cache
def get_outputs(
    dht: DHT, node_key: str, r, s, get_cached_fn=None
) -> dict[str, tuple[float, dict]]:  # Q: (timestamp, outputs)
    # Try provided cache function first.
    if get_cached_fn:
        if outputs := get_cached_fn(r, s):
            return hash_keys(outputs)

    # Try from DHT next to include peered outputs.
    if outputs := get_dht_value(dht, key=outputs_key(node_key, r, s), latest=False):
        return hash_keys(outputs)

    raise ValueError(
        f"could not retrieve stage outputs for {node_key} at round {r} stage {s}"
    )


def get_round_and_stage(
    dht: DHT,
) -> tuple[int, int]:
    value = get_dht_value(dht, key=ROUND_STAGE_NUMBER_KEY, latest=True)
    if not value:
        raise ValueError("cannot find current round and stage")

    round_num, stage = value
    return round_num, stage


def get_dht_value(dht: DHT, **kwargs) -> Any | None:
    wrapper = dht.get(**kwargs)
    if not wrapper:
        return None

    assert isinstance(wrapper, ValueWithExpiration)
    value = wrapper.value
    if isinstance(value, dict):
        # Subkeys exist; unwrap ValueWithExpiration.
        return {k: v.value for k, v in value.items()}
    return value
