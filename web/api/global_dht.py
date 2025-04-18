import multiprocessing

import hivemind

from . import server_cache

# DHT singletons for the client
# Initialized in main and used in the API handlers.
dht: hivemind.DHT | None = None
dht_cache: server_cache.Cache | None = None


def setup_global_dht(initial_peers, coordinator, logger, kinesis_client):
    global dht
    global dht_cache
    dht = hivemind.DHT(
        start=True,
        startup_timeout=60,
        initial_peers=initial_peers,
        cache_nearest=2,
        cache_size=2000,
        client_mode=True,
    )
    dht_cache = server_cache.Cache(
        dht, coordinator, multiprocessing.Manager(), logger, kinesis_client
    )
