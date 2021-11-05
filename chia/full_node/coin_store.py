from typing import List, Optional, Set, Dict
import aiosqlite
import asyncio
from chia.protocols.wallet_protocol import CoinState
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_record import CoinRecord
from chia.util.db_wrapper import DBWrapper
from chia.util.ints import uint32, uint64
from chia.util.lru_cache import LRUCache
from time import time
import logging

log = logging.getLogger(__name__)


class CoinStore:
    """
    This object handles CoinRecords in DB.
    A cache is maintained for quicker access to recent coins.
    """

    coin_record_db: aiosqlite.Connection
    coin_record_cache: LRUCache
    cache_size: uint32
    db_wrapper: DBWrapper

    # TODO(stonemelody): This should really be a reader/writer lock.
    lock: asyncio.Lock
    coins_by_name: Dict[bytes32, CoinRecord]
    coins_by_added: Dict[uint32, List[CoinRecord]]
    coins_by_removed: Dict[uint32, List[CoinRecord]]
    coins_by_puzzle_hash: Dict[bytes32, List[CoinRecord]]
    coins_by_parent_id: Dict[bytes32, List[CoinRecord]]
    # Coins that were spent, but were not in the cache that we had.
    # TODO(stonemelody): This will essentially require a set intersection
    # operation with resutls fetched from the database later.
    coins_spent_uncached_by_height: Dict[uint32, List[CoinRecord]]

    @classmethod
    async def create(cls, db_wrapper: DBWrapper, cache_size: uint32 = uint32(60000)):
        self = cls()

        self.coins_by_name = {}
        self.coins_by_added = {}
        self.coins_by_removed = {}
        self.coins_by_puzzle_hash = {}
        self.coins_by_parent_id = {}

        self.cache_size = cache_size
        self.db_wrapper = db_wrapper
        self.coin_record_db = db_wrapper.db
        # the coin_name is unique in this table because the CoinStore always
        # only represent a single peak
        await self.coin_record_db.execute(
            (
                "CREATE TABLE IF NOT EXISTS coin_record("
                "coin_name text PRIMARY KEY,"
                " confirmed_index bigint,"
                " spent_index bigint,"
                " spent int,"
                " coinbase int,"
                " puzzle_hash text,"
                " coin_parent text,"
                " amount blob,"
                " timestamp bigint)"
            )
        )

        # Useful for reorg lookups
        await self.coin_record_db.execute(
            "CREATE INDEX IF NOT EXISTS coin_confirmed_index on coin_record(confirmed_index)"
        )

        await self.coin_record_db.execute("CREATE INDEX IF NOT EXISTS coin_spent_index on coin_record(spent_index)")

        await self.coin_record_db.execute("CREATE INDEX IF NOT EXISTS coin_spent on coin_record(spent)")

        await self.coin_record_db.execute("CREATE INDEX IF NOT EXISTS coin_puzzle_hash on coin_record(puzzle_hash)")

        await self.coin_record_db.execute("CREATE INDEX IF NOT EXISTS coin_parent_index on coin_record(coin_parent)")

        await self.coin_record_db.commit()
        self.coin_record_cache = LRUCache(cache_size)
        return self

    async def new_block(
        self,
        height: uint32,
        timestamp: uint64,
        included_reward_coins: Set[Coin],
        tx_additions: List[Coin],
        tx_removals: List[bytes32],
    ) -> List[CoinRecord]:
        """
        Only called for blocks which are blocks (and thus have rewards and transactions)
        Returns a list of the CoinRecords that were added by this block
        """

        start = time()

        additions = []

        async with self.lock:
            for coin in tx_additions:
                record: CoinRecord = CoinRecord(
                    coin,
                    height,
                    uint32(0),
                    False,
                    False,
                    timestamp,
                )

                self.coin_record_cache.put(record.coin.name(), record)
                self._add_to_store_cache(record)
                additions.append(record)

            if height == 0:
                assert len(included_reward_coins) == 0
            else:
                assert len(included_reward_coins) >= 2

            for coin in included_reward_coins:
                reward_coin_r: CoinRecord = CoinRecord(
                    coin,
                    height,
                    uint32(0),
                    False,
                    True,
                    timestamp,
                )
                self.coin_record_cache.put(reward_coin_r.coin.name(), record)
                self._add_to_store_cache(reward_coin_r)
                additions.append(reward_coin_r)

            remaining_removals = []
            for spent in tx_removals:
                updated = False
                record = self.coins_by_name.get(spent, None)
                if record is not None:
                    record.spent = True
                    record.spent_block_index = height
                    updated = True

                r = self.coin_record_cache.get(spent)
                if r is not None:
                    new_record = CoinRecord(
                        r.coin,
                        r.confirmed_block_index,
                        index,
                        True,
                        r.coinbase,
                        r.timestamp
                    )
                    self.coin_record_cache.put(r.name, new_record)

                    # The goal is to put off doing database writes as long as
                    # possible, so we should prefer doing reads and pulling data
                    # from the cache if we didn't already have the coin in our
                    # store cache dict.
                    if not updated:
                        self._add_to_store_cache(new_record)
                        updated = True

                if not updated:
                    remaining_removals.append(spent)

            # Pull coins not in either cache from the store.
            read = self.get_coin_records_by_names_from_db(remaining_removals)
            if record in read:
                record.spent_block_index = index
                record.spent = True
                self._add_to_store_cache(record)

        end = time()

        return additions

    # Checks DB and DiffStores for CoinRecord with coin_name and returns it
    async def get_coin_record(self, coin_name: bytes32) -> Optional[CoinRecord]:
        cached = self.coin_record_cache.get(coin_name)
        if cached is not None:
            return cached

        async with self.lock:
            cached = self.coins_by_name.get(coin_name, None)
            if cached is not None:
                return cached

        cursor = await self.coin_record_db.execute("SELECT * from coin_record WHERE coin_name=?", (coin_name.hex(),))
        row = await cursor.fetchone()
        await cursor.close()
        if row is not None:
            coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
            record = CoinRecord(coin, row[1], row[2], row[3], row[4], row[8])
            self.coin_record_cache.put(record.coin.name(), record)
            return record
        return None

    # TODO(stonemelody): This is very similar to `get_coin_records_by_names()`
    # except the latter does not:
    #   * return None if a coin is not found
    #   * check the cache
    #   * add the found coin to the cache
    async def get_coin_records(self, coin_names: List[bytes32] -> List[Optional[CoinRecord]]:
        remaining: List[bytes32] = []
        ret: List[Optional[CoinRecord]] = []
        async with self.lock:
            for coin_name in coin_names:
                cached = self.coin_record_cache.get(coin_name)
                if cached is not None:
                    ret[coin_name] = cached
                    continue
                cached = self.coins_by_name(coin_name)
                if cached is not None:
                    ret[coin_name] = cached
                    continue
                remaining.append(coin_name)

        if len(remaining) == 0:
            return ret

        cursor = await self.coin_record_db.execute(
            f'SELECT * from coin_record WHERE coin_name in ({"}," * (len(remaining) - 1)}?)')
        rows = await cursor.fetchall()
        await cursor.close()

        found = set()
        for row in rows:
            if row is not None:
                coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
                record = CoinRecord(coin, row[1], row[2], row[3], row[4], row[8])
                self.coin_record_cache.put(record.coin.name(), record)
                ret.append(record)
                found.add(record.coin.name())

        # Add None for coins that were not in the cache or database
        ret.extend([None for x in set(reamining) - found])
        return ret

    async def get_coins_added_at_height(self, height: uint32) -> List[CoinRecord]:
        coins = []
        async with self.lock:
            cached = self.coins_by_added.get(height, None)
            if cached is not None:
                # It should be safe to return here without checking the database
                # because we assume that coins are added only when the block
                # confirming them is added, and that coins exist in either the
                # cache or the database but not both.
                return list(cached.values())

        cursor = await self.coin_record_db.execute("SELECT * from coin_record WHERE confirmed_index=?", (height,))
        rows = await cursor.fetchall()
        await cursor.close()
        for row in rows:
            coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
            coins.append(CoinRecord(coin, row[1], row[2], row[3], row[4], row[8]))
        return coins

    async def get_coins_removed_at_height(self, height: uint32) -> List[CoinRecord]:
        # Special case to avoid querying all unspent coins (spent_index=0)
        if height == 0:
            return []

        async with self.lock:
            cached = self.coins_by_removed.get(height, None)
            if cached is not None:
                # Same logic as `get_coins_added_at_height()` for why this is
                # ok.
                return cached

        cursor = await self.coin_record_db.execute("SELECT * from coin_record WHERE spent_index=?", (height,))
        rows = await cursor.fetchall()
        await cursor.close()
        coins = []
        for row in rows:
            spent: bool = bool(row[3])
            if spent:
                coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
                coin_record = CoinRecord(coin, row[1], row[2], spent, row[4], row[8])
                coins.append(coin_record)
        return coins

    # Checks DB and DiffStores for CoinRecords with puzzle_hash and returns them
    async def get_coin_records_by_puzzle_hash(
        self,
        include_spent_coins: bool,
        puzzle_hash: bytes32,
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2 ** 32) - 1),
    ) -> List[CoinRecord]:
        return await self.get_coin_records_by_puzzle_hashes(
                include_spent_coins,
                [puzzle_hash],
                start_height,
                end_height)

    async def get_coin_records_by_puzzle_hashes(
        self,
        include_spent_coins: bool,
        puzzle_hashes: List[bytes32],
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2 ** 32) - 1),
    ) -> List[CoinRecord]:
        if len(puzzle_hashes) == 0:
            return []

        coins = {}
        async with self.lock:
            for puzzle_hash in puzzle_hashes:
                cached = self.coins_by_puzzle_hash.get(puzzle_hash, None)
                if cached is not None:
                    coins.update({x.name: x for x in cached if (
                        x.confirmed_index >= start_height and
                        x.confirmed_index < end_height and
                        (include_spent_coins or (not include_spent_coins and not x.spent))
                    )})

        puzzle_hashes_db = tuple([ph.hex() for ph in puzzle_hashes])
        cursor = await self.coin_record_db.execute(
            f"SELECT * from coin_record INDEXED BY coin_puzzle_hash "
            f'WHERE puzzle_hash in ({"?," * (len(puzzle_hashes_db) - 1)}?) '
            f"AND confirmed_index>=? AND confirmed_index<? "
            f"{'' if include_spent_coins else 'AND spent=0'}",
            puzzle_hashes_db + (start_height, end_height),
        )

        rows = await cursor.fetchall()

        await cursor.close()
        for row in rows:
            coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
            if coin.name() not in coins:
                coins[coin.name()] = CoinRecord(coin, row[1], row[2], row[3], row[4], row[8])
        return list(coins.values())

    async def get_coin_records_by_names(
        self,
        include_spent_coins: bool,
        names: List[bytes32],
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2 ** 32) - 1),
    ) -> List[CoinRecord]:
        if len(names) == 0:
            return []

        remaining = []
        coins = set()
        async with self.lock:
            for name in names:
                cached = self.coins_by_name.get(name, None)
                if cached is not None:
                    coins.update([x for x in cached if (
                        x.confirmed_index >= start_height and
                        x.confirmed_index < end_height and
                        (include_spent_coins or (not include_spent_coins and not x.spent))
                    )])
                else:
                    remaining.append(name)
        coins.update(self.get_coin_records_by_names_from_db(remaining))
        return list(coins)

    async def get_coin_records_by_names_from_db(
        self,
        include_spent_coins: bool,
        names: List[bytes32],
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2 ** 32) - 1),
    ) -> Set[CoinRecord]:
        names_db = tuple([name.hex() for name in names])
        cursor = await self.coin_record_db.execute(
            f'SELECT * from coin_record WHERE coin_name in ({"?," * (len(names_db) - 1)}?) '
            f"AND confirmed_index>=? AND confirmed_index<? "
            f"{'' if include_spent_coins else 'AND spent=0'}",
            names_db + (start_height, end_height),
        )
        rows = await cursor.fetchall()

        await cursor.close()
        for row in rows:
            coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
            coins.add(CoinRecord(coin, row[1], row[2], row[3], row[4], row[8]))

        return coins

    def row_to_coin_state(self, row):
        coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
        spent_h = None
        if row[3]:
            spent_h = row[2]
        return CoinState(coin, spent_h, row[1])

    async def get_coin_states_by_puzzle_hashes(
        self,
        include_spent_coins: bool,
        puzzle_hashes: List[bytes32],
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2 ** 32) - 1),
    ) -> List[CoinState]:
        if len(puzzle_hashes) == 0:
            return []

        coins = {}
        async with self.lock:
            for puzzle_hash in puzzle_hashes:
                cached = self.coins_by_puzzle_hash.get(puzzle_hash, None)
                if cached is not None:
                    coins.update({
                        x.name: CoinState(x, x.spent_block_index, x.confirmed_block_index)
                        for x in cached if (
                            x.confirmed_index >= start_height and
                            x.confirmed_index < end_height and
                            (include_spent_coins or (not include_spent_coins and not x.spent))
                    )})

        puzzle_hashes_db = tuple([ph.hex() for ph in puzzle_hashes])
        cursor = await self.coin_record_db.execute(
            f'SELECT * from coin_record WHERE puzzle_hash in ({"?," * (len(puzzle_hashes_db) - 1)}?) '
            f"AND confirmed_index>=? AND confirmed_index<? "
            f"{'' if include_spent_coins else 'AND spent=0'}",
            puzzle_hashes_db + (start_height, end_height),
        )

        rows = await cursor.fetchall()

        await cursor.close()
        for row in rows:
            cr = self.row_to_coin_state(row)
            if cr.name not in coins:
                coins[cr.name] = cr

        return list(coins.values())

    async def get_coin_records_by_parent_ids(
        self,
        include_spent_coins: bool,
        parent_ids: List[bytes32],
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2 ** 32) - 1),
    ) -> List[CoinRecord]:
        if len(parent_ids) == 0:
            return []

        coins = {}
        async with self.lock:
            for pid in parent_ids:
                cached = self.coins_by_parent_id.get(pid, None)
                if cached is not None:
                    coins.update({
                        x.name: CoinState(x, x.spent_block_index, x.confirmed_block_index)
                        for x in cached if (
                            x.confirmed_index >= start_height and
                            x.confirmed_index < end_height and
                            (include_spent_coins or (not include_spent_coins and not x.spent))
                    )})

        parent_ids_db = tuple([pid.hex() for pid in parent_ids])
        cursor = await self.coin_record_db.execute(
            f'SELECT * from coin_record WHERE coin_parent in ({"?," * (len(parent_ids_db) - 1)}?) '
            f"AND confirmed_index>=? AND confirmed_index<? "
            f"{'' if include_spent_coins else 'AND spent=0'}",
            parent_ids_db + (start_height, end_height),
        )

        rows = await cursor.fetchall()

        await cursor.close()
        for row in rows:
            coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
            if coin.name() not in coins:
                coins[coin.name()] = CoinRecord(coin, row[1], row[2], row[3], row[4], row[8])
        return list(coins)

    async def get_coin_state_by_ids(
        self,
        include_spent_coins: bool,
        coin_ids: List[bytes32],
        start_height: uint32 = uint32(0),
        end_height: uint32 = uint32((2 ** 32) - 1),
    ) -> List[CoinState]:
        if len(coin_ids) == 0:
            return []

        # Coin names are unique, so we don't need to look them up in the
        # database if we find them in the database cache.
        remaining = []
        coins = set()
        async with self.lock:
            for name in coin_ids:
                cached = self.coins_by_name.get(name, None)
                if cached is not None:
                    coins.update([
                        CoinState(x, x.spent_block_index, x.confirmed_block_index)
                        for x in cached if (
                            x.confirmed_index >= start_height and
                            x.confirmed_index < end_height and
                            (include_spent_coins or (not include_spent_coins and not x.spent))
                    )])
                else:
                    remaining.append(name)

        if len(remaining) == 0:
            return list(coins)

        parent_ids_db = tuple([pid.hex() for pid in coin_ids])
        cursor = await self.coin_record_db.execute(
            f'SELECT * from coin_record WHERE coin_name in ({"?," * (len(parent_ids_db) - 1)}?) '
            f"AND confirmed_index>=? AND confirmed_index<? "
            f"{'' if include_spent_coins else 'AND spent=0'}",
            parent_ids_db + (start_height, end_height),
        )

        rows = await cursor.fetchall()

        await cursor.close()
        for row in rows:
            coins.add(self.row_to_coin_state(row))
        return list(coins)

    async def rollback_to_block(self, block_index: int) -> List[CoinRecord]:
        """
        Note that block_index can be negative, in which case everything is rolled back
        Returns the list of coin records that have been modified
        """
        # Update memory cache
        delete_queue: bytes32 = []
        for coin_name, coin_record in list(self.coin_record_cache.cache.items()):
            if int(coin_record.confirmed_block_index) > block_index:
                delete_queue.append(coin_name)
            elif int(coin_record.spent_block_index) > block_index:
                # No need to update coins that will be removed from the cache,
                # so safe to check this only if the first condition is not true.
                new_record = CoinRecord(
                    coin_record.coin,
                    coin_record.confirmed_block_index,
                    uint32(0),
                    False,
                    coin_record.coinbase,
                    coin_record.timestamp,
                )
                self.coin_record_cache.put(coin_record.coin.name(), new_record)

        for coin_name in delete_queue:
            self.coin_record_cache.remove(coin_name)

        coin_changes: Dict[bytes32, CoinRecord] = {}
        async with self.lock:
            records = []
            for idx, r in self.coins_by_added.items():
                if idx > block_index:
                    records.extend(r.values())

            for coin_record in records:
                self._remove_from_store_cache(coin_record)
                coin_record.confirmed_block_index = 0
                coin_record.timestamp = 0
                coin_changes[coin_record.name] = coin_record

            records.clear()
            for idx, r in self.coins_by_removed.items():
                if idx > block_index:
                    for coin_record in r.values():
                        coin_record.spent_block_index = 0
                        coin_record.spent = False
                        if coin_record.name not in coin_changes:
                            coin_changes[coin_record.name] = coin_record


        cursor_deleted = await self.coin_record_db.execute(
            "SELECT * FROM coin_record WHERE confirmed_index>?", (block_index,)
        )
        rows = await cursor_deleted.fetchall()
        for row in rows:
            # There could be a small chance that we'll rollback stuff we just
            # added, but before we cleared the cache. This occurs because we
            # don't hold the cache lock while adding stuff to the database.
            coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
            if coin.name() not in coin_changes:
                record = CoinRecord(coin, uint32(0), row[2], row[3], row[4], uint64(0))
                coin_changes[record.name] = record
        await cursor_deleted.close()

        # Delete from storage
        c1 = await self.coin_record_db.execute("DELETE FROM coin_record WHERE confirmed_index>?", (block_index,))
        await c1.close()

        cursor_unspent = await self.coin_record_db.execute(
            "SELECT * FROM coin_record WHERE confirmed_index>?", (block_index,)
        )
        rows = await cursor_unspent.fetchall()
        for row in rows:
            coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
            record = CoinRecord(coin, row[1], uint32(0), False, row[4], row[8])
            if record.name not in coin_changes:
                coin_changes[record.name] = record
        await cursor_unspent.close()

        c2 = await self.coin_record_db.execute(
            "UPDATE coin_record SET spent_index = 0, spent = 0 WHERE spent_index>?",
            (block_index,),
        )
        await c2.close()
        return list(coin_changes.values())

    # Add a coin to the in-memory cache for the CoinStore. The lock on this
    # object must be held before calling this function.
    def _add_to_store_cache(self, record) -> None:
        if self.coins_by_name(record.name, None) is not None:
            # TODO(stonemelody): Raise some error about the coin already
            # existing.
            raise
        self.coins_by_name[record.name] = record

        if self.coins_by_added.get(record.confirmed_block_index, None) is None:
            self.coins_by_added[record.confirmed_block_index] = {}
        self.coins_by_added[record.confirmed_block_index][record.name] = record

        if self.coins_by_removed.get(record.spent_block_index, None) is None:
            self.coins_by_removed[record.spent_block_index] = {}
        self.coins_by_removed[record.spent_block_index].record.name] = record

        if self.coins_by_puzzle_hash(record.coin.puzzle_hash, None) is None:
            self.coins_by_puzzle_hash[record.coin.puzzle_hash] = {}
        self.coins_by_puzzle_hash[record.coin.puzzle_hash][record.name] = record

        if self.coins_by_parent_id(record.coin.parent_coin_info, None) is None:
            self.coins_by_parent_id[record.coin.parent_coin_info] = {}
        self.coins_by_parent_id[record.coin.parent_coin_info][record.name] = record

    def _remove_from_store_cache(self, record) -> None:
        self.coins_by_name.pop(record.name, None)

        records = self.coins_by_added.get(record.confirmed_block_index, None)
        if records is not None:
            records.pop(record.name, None)
            if len(records) == 0:
                del self.coins_by_added[record.confirmed_block_index]

        records = self.coins_by_removed.get(record.spent_block_index, None)
        if records is not None:
            records.pop(record.name, None)
            if len(records) == 0:
                del self.coins_by_removed[record.spent_block_index]

        records = self.coins_by_puzzle_hash.get(record.coin.puzzle_hash, None)
        if records is not None:
            records.pop(record.name, None)
            if len(records) == 0:
                del self.coins_by_puzzle_hash[record.coin.puzzle_hash]

        records = self.coins_by_parent_id.get(record.coin.parent_coin_info, None)
        if records is not None:
            records.pop(record.name, None)
            if len(records) == 0:
                del self.coins_by_parent_id[record.coin.parent_coin_info]

    # Store CoinRecord in DB and ram cache
    async def store_coins(self) -> None:

        values = []
        async with self.lock:
            for record in self.coins_by_name.values():
                values.append(
                    (
                        record.name.hex(),
                        record.confirmed_block_index,
                        record.spent_block_index,
                        int(record.spent),
                        int(record.coinbase),
                        str(record.coin.puzzle_hash.hex()),
                        str(record.coin.parent_coin_info.hex()),
                        bytes(record.coin.amount),
                        record.timestamp,
                    )
                )

        cursor = await self.coin_record_db.executemany(
            "INSERT OR REPLACE INTO coin_record VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values,
        )
        await cursor.close()

        async with self.lock:
            self.coins_by_name.clear()
            self.coins_by_added.clear()
            self.coins_by_removed.clear()
            self.coins_by_puzzle_hash.clear()
            self.coins_by_parent_id.clear()
