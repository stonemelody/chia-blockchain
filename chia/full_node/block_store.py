import logging
from typing import Dict, List, Optional, Tuple

import aiosqlite
import asyncio

from chia.consensus.block_record import BlockRecord
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.blockchain_format.sub_epoch_summary import SubEpochSummary
from chia.types.full_block import FullBlock
from chia.types.weight_proof import SubEpochChallengeSegment, SubEpochSegments
from chia.util.db_wrapper import DBWrapper
from chia.util.ints import uint32
from chia.util.lru_cache import LRUCache

log = logging.getLogger(__name__)


class BlockData:
    header_hash: bytes32
    block: FullBlock
    block_record: BlockRecord

    def __init__(self, header_hash: bytes32, block: FullBlock, block_record: BlockRecord):
        self.header_hash = header_hash
        self.block = block
        self.block_record = block_record

class StoreBlock:
    block: FullBlock
    block_record: BlockRecord
    is_peak: bool

    def __init__(self, block: FullBlock, block_record: BlockRecord, is_peak: bool):
        self.block = block
        self.block_record = block_record
        self.is_peak = is_peak


class BlockStore:
    db: aiosqlite.Connection
    block_cache: LRUCache
    db_wrapper: DBWrapper
    ses_challenge_cache: LRUCache

    # TODO(stonemelody): This should really be a reader/writer lock.
    lock: asyncio.Lock
    store_cache: Dict[bytes32, StoreBlock]
    store_cache_heights: Dict[uint32, List[StoreBlock]]
    last_peak: bytes32

    @classmethod
    async def create(cls, db_wrapper: DBWrapper):
        self = cls()
        self.last_peak = None
        self.store_cache = {}
        self.store_cache_heights = {}
        self.lock = asyncio.Lock()

        # All full blocks which have been added to the blockchain. Header_hash -> block
        self.db_wrapper = db_wrapper
        self.db = db_wrapper.db
        await self.db.execute(
            "CREATE TABLE IF NOT EXISTS full_blocks(header_hash text PRIMARY KEY, height bigint,"
            "  is_block tinyint, is_fully_compactified tinyint, block blob)"
        )

        # Block records
        await self.db.execute(
            "CREATE TABLE IF NOT EXISTS block_records(header_hash "
            "text PRIMARY KEY, prev_hash text, height bigint,"
            "block blob, sub_epoch_summary blob, is_peak tinyint, is_block tinyint)"
        )

        # todo remove in v1.2
        await self.db.execute("DROP TABLE IF EXISTS sub_epoch_segments_v2")

        # Sub epoch segments for weight proofs
        await self.db.execute(
            "CREATE TABLE IF NOT EXISTS sub_epoch_segments_v3(ses_block_hash text PRIMARY KEY, challenge_segments blob)"
        )

        # Height index so we can look up in order of height for sync purposes
        await self.db.execute("CREATE INDEX IF NOT EXISTS full_block_height on full_blocks(height)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS is_block on full_blocks(is_block)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS is_fully_compactified on full_blocks(is_fully_compactified)")

        await self.db.execute("CREATE INDEX IF NOT EXISTS height on block_records(height)")

        await self.db.execute("CREATE INDEX IF NOT EXISTS hh on block_records(header_hash)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS peak on block_records(is_peak)")
        await self.db.execute("CREATE INDEX IF NOT EXISTS is_block on block_records(is_block)")

        await self.db.commit()
        self.block_cache = LRUCache(1000)
        self.ses_challenge_cache = LRUCache(50)
        return self

    async def add_full_block(self, block: BlockData) -> None:
        await self.add_full_blocks([block])

    async def add_full_blocks(self, blocks: List[BlockData]) -> None:
        async with self.lock:
            for block in blocks:
                self.block_cache.put(block.header_hash, block.block)

                b = StoreBlock(block, block.block_record, False)
                self.store_cache[block.header_hash] = b
                if self.store_cache_heights.get(block.height, None) is None:
                    self.store_cache_heights[block.height] = []
                self.store_cache_heights[block.height].append(b)

    async def store_full_blocks(self) -> None:
        cursor_1 = await self.db.executemany(
            "INSERT OR REPLACE INTO full_blocks VALUES(?, ?, ?, ?, ?)",
            [(
                header_hash.hex(),
                block.block.height,
                int(block.block.is_transaction_block()),
                int(block.block.is_fully_compactified()),
                bytes(block.block),
             ) for header_hash, block in self.store_cache],
        )

        await cursor_1.close()

        cursor_2 = await self.db.executemany(
            "INSERT OR REPLACE INTO block_records VALUES(?, ?, ?, ?,?, ?, ?)",
            [(
                header_hash.hex(),
                block.block.prev_header_hash.hex(),
                block.block.height,
                bytes(block.block_record),
                None
                if block.block_record.sub_epoch_summary_included is None
                else bytes(block.block_record.sub_epoch_summary_included),
                block.is_peak,
                block.block.is_transaction_block(),
            ) for block in self.store_cache],
        )
        await cursor_2.close()

        async with self.lock:
            self.store_cache.clear()
            self.store_cache_heights.clear()

    async def persist_sub_epoch_challenge_segments(
        self, ses_block_hash: bytes32, segments: List[SubEpochChallengeSegment]
    ) -> None:
        async with self.db_wrapper.lock:
            cursor_1 = await self.db.execute(
                "INSERT OR REPLACE INTO sub_epoch_segments_v3 VALUES(?, ?)",
                (ses_block_hash.hex(), bytes(SubEpochSegments(segments))),
            )
            await cursor_1.close()
            await self.db.commit()

    async def get_sub_epoch_challenge_segments(
        self,
        ses_block_hash: bytes32,
    ) -> Optional[List[SubEpochChallengeSegment]]:
        cached = self.ses_challenge_cache.get(ses_block_hash)
        if cached is not None:
            return cached
        cursor = await self.db.execute(
            "SELECT challenge_segments from sub_epoch_segments_v3 WHERE ses_block_hash=?", (ses_block_hash.hex(),)
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is not None:
            challenge_segments = SubEpochSegments.from_bytes(row[0]).challenge_segments
            self.ses_challenge_cache.put(ses_block_hash, challenge_segments)
            return challenge_segments
        return None

    def rollback_cache_block(self, header_hash: bytes32):
        try:
            self.block_cache.remove(header_hash)
        except KeyError:
            # this is best effort. When rolling back, we may not have added the
            # block to the cache yet
            pass

    def rollback_store_block(self, header_hash: bytes32):
        async with self.lock:
            cached = self.store_cache.get(header_hash, None)
            if cached is not None:
                del self.store_cache_heights[cached.block.height]
                del self.store_cache[header_hash]

    async def get_full_block(self, header_hash: bytes32) -> Optional[FullBlock]:
        cached = self.block_cache.get(header_hash)
        if cached is not None:
            log.debug(f"cache hit for block {header_hash.hex()}")
            return cached
        async with self.lock:
            cached = self.store_cache.get(header_hash, None)
            if cached is not None:
                return cached.block
        log.debug(f"cache miss for block {header_hash.hex()}")
        cursor = await self.db.execute("SELECT block from full_blocks WHERE header_hash=?", (header_hash.hex(),))
        row = await cursor.fetchone()
        await cursor.close()
        if row is not None:
            block = FullBlock.from_bytes(row[0])
            self.block_cache.put(header_hash, block)
            return block
        return None

    async def get_full_block_bytes(self, header_hash: bytes32) -> Optional[bytes]:
        cached = self.block_cache.get(header_hash)
        if cached is not None:
            log.debug(f"cache hit for block {header_hash.hex()}")
            return bytes(cached)
        async with self.lock:
            cached = self.store_cache.get(header_hash, None)
            if cached is not None:
                return bytes(cached.block)
        log.debug(f"cache miss for block {header_hash.hex()}")
        cursor = await self.db.execute("SELECT block from full_blocks WHERE header_hash=?", (header_hash.hex(),))
        row = await cursor.fetchone()
        await cursor.close()
        if row is not None:
            return row[0]
        return None

    async def get_full_blocks_at(self, heights: List[uint32]) -> List[FullBlock]:
        if len(heights) == 0:
            return []

        ret: List[FullBlock] = []
        if len(self.store_cache_heights) > 0:
            async with self.lock:
                for h in heights:
                    cached = self.store_cache_heights.get(h, None)
                    if cached is not None:
                        ret.extend([b.block for b in cached])

        heights_db = tuple(heights)
        formatted_str = f'SELECT block from full_blocks WHERE height in ({"?," * (len(heights_db) - 1)}?)'
        cursor = await self.db.execute(formatted_str, heights_db)
        rows = await cursor.fetchall()
        await cursor.close()
        ret.extend([FullBlock.from_bytes(row[0] for row in rows)])
        return ret

    async def get_block_records_by_hash(self, header_hashes: List[bytes32]):
        """
        Returns a list of Block Records, ordered by the same order in which header_hashes are passed in.
        Throws an exception if the blocks are not present
        """
        if len(header_hashes) == 0:
            return []

        all_blocks: Dict[bytes32, BlockRecord] = {}
        remaining: List[bytes32] = []
        if len(self.store_cache) > 0:
            async with self.lock:
                for hh in header_hashes:
                    cached = self.store_cache.get(hh, None)
                    if cached is not None:
                        all_blocks[hh] = cached.block_record
                    else:
                        remaining.append(hh)
        else:
            remaining = header_hashes

        header_hashes_db = tuple([hh.hex() for hh in remaining])
        formatted_str = f'SELECT block from block_records WHERE header_hash in ({"?," * (len(header_hashes_db) - 1)}?)'
        cursor = await self.db.execute(formatted_str, header_hashes_db)
        rows = await cursor.fetchall()
        await cursor.close()
        for row in rows:
            block_rec: BlockRecord = BlockRecord.from_bytes(row[0])
            all_blocks[block_rec.header_hash] = block_rec
        ret: List[BlockRecord] = []
        for hh in header_hashes:
            if hh not in all_blocks:
                raise ValueError(f"Header hash {hh} not in the blockchain")
            ret.append(all_blocks[hh])
        return ret

    async def get_blocks_by_hash(self, header_hashes: List[bytes32]) -> List[FullBlock]:
        """
        Returns a list of Full Blocks blocks, ordered by the same order in which header_hashes are passed in.
        Throws an exception if the blocks are not present
        """

        if len(header_hashes) == 0:
            return []

        all_blocks: Dict[bytes32, FullBlock] = {}
        remaining: List[bytes32] = []
        if len(self.store_cache) > 0:
            async with self.lock:
                for hh in header_hashes:
                    cached = self.store_cache.get(hh, None)
                    if cached is not None:
                        all_blocks[hh] = cached.block
                    else:
                        remaining.append(hh)
        else:
            remaining = header_hashes

        header_hashes_db = tuple([hh.hex() for hh in remaining])
        formatted_str = (
            f'SELECT header_hash, block from full_blocks WHERE header_hash in ({"?," * (len(header_hashes_db) - 1)}?)'
        )
        cursor = await self.db.execute(formatted_str, header_hashes_db)
        rows = await cursor.fetchall()
        await cursor.close()
        for row in rows:
            header_hash = bytes.fromhex(row[0])
            full_block: FullBlock = FullBlock.from_bytes(row[1])
            all_blocks[header_hash] = full_block
            self.block_cache.put(header_hash, full_block)
        ret: List[FullBlock] = []
        for hh in header_hashes:
            if hh not in all_blocks:
                raise ValueError(f"Header hash {hh} not in the blockchain")
            ret.append(all_blocks[hh])
        return ret

    async def get_block_record(self, header_hash: bytes32) -> Optional[BlockRecord]:
        async with self.lock:
            cached = self.store_cache.get(header_hash, None)
            if cached is not None:
                return cached.block_record

        cursor = await self.db.execute(
            "SELECT block from block_records WHERE header_hash=?",
            (header_hash.hex(),),
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is not None:
            return BlockRecord.from_bytes(row[0])
        return None

    async def get_block_records_in_range(
        self,
        start: int,
        stop: int,
    ) -> Dict[bytes32, BlockRecord]:
        """
        Returns a dictionary with all blocks in range between start and stop
        if present.
        """

        ret: Dict[bytes32, BlockRecord] = {}
        async with self.lock:
            for b in self.store_cache_heights.sort(key=lambda x: x.block.height):
                if b.block.height >= start and b.block.height <= stop:
                    ret[b.block.header_hash] = b.block_record
                elif b.block.height > stop:
                    break

        formatted_str = f"SELECT header_hash, block from block_records WHERE height >= {start} and height <= {stop}"

        cursor = await self.db.execute(formatted_str)
        rows = await cursor.fetchall()
        await cursor.close()
        for row in rows:
            header_hash = bytes.fromhex(row[0])
            ret[header_hash] = BlockRecord.from_bytes(row[1])

        return ret

    async def get_block_records_close_to_peak(
        self, blocks_n: int
    ) -> Tuple[Dict[bytes32, BlockRecord], Optional[bytes32]]:
        """
        Returns a dictionary with all blocks that have height >= peak height - blocks_n, as well as the
        peak header hash. Only called when initializing the blockchain, so the
        store cache will be empty.
        """

        res = await self.db.execute("SELECT * from block_records WHERE is_peak = 1")
        peak_row = await res.fetchone()
        await res.close()
        if peak_row is None:
            return {}, None

        formatted_str = f"SELECT header_hash, block  from block_records WHERE height >= {peak_row[2] - blocks_n}"
        cursor = await self.db.execute(formatted_str)
        rows = await cursor.fetchall()
        await cursor.close()
        ret: Dict[bytes32, BlockRecord] = {}
        for row in rows:
            header_hash = bytes.fromhex(row[0])
            ret[header_hash] = BlockRecord.from_bytes(row[1])
        return ret, bytes.fromhex(peak_row[0])

    async def get_peak_height_dicts(self) -> Tuple[Dict[uint32, bytes32], Dict[uint32, SubEpochSummary]]:
        """
        Returns a dictionary with all blocks, as well as the header hash of the peak,
        if present. Only called when initializing the blockchain, so the store
        cache will be empty.
        """

        res = await self.db.execute("SELECT * from block_records WHERE is_peak = 1")
        row = await res.fetchone()
        await res.close()
        if row is None:
            return {}, {}

        peak: bytes32 = bytes.fromhex(row[0])
        cursor = await self.db.execute("SELECT header_hash,prev_hash,height,sub_epoch_summary from block_records")
        rows = await cursor.fetchall()
        await cursor.close()
        hash_to_prev_hash: Dict[bytes32, bytes32] = {}
        hash_to_height: Dict[bytes32, uint32] = {}
        hash_to_summary: Dict[bytes32, SubEpochSummary] = {}

        for row in rows:
            hash_to_prev_hash[bytes.fromhex(row[0])] = bytes.fromhex(row[1])
            hash_to_height[bytes.fromhex(row[0])] = row[2]
            if row[3] is not None:
                hash_to_summary[bytes.fromhex(row[0])] = SubEpochSummary.from_bytes(row[3])

        height_to_hash: Dict[uint32, bytes32] = {}
        sub_epoch_summaries: Dict[uint32, SubEpochSummary] = {}

        curr_header_hash = peak
        curr_height = hash_to_height[curr_header_hash]
        while True:
            height_to_hash[curr_height] = curr_header_hash
            if curr_header_hash in hash_to_summary:
                sub_epoch_summaries[curr_height] = hash_to_summary[curr_header_hash]
            if curr_height == 0:
                break
            curr_header_hash = hash_to_prev_hash[curr_header_hash]
            curr_height = hash_to_height[curr_header_hash]
        return height_to_hash, sub_epoch_summaries

    async def set_peak(self, header_hash: bytes32) -> None:
        async with self.lock:
            cached = self.store_cache.get(header_hash)
            if cached is not None:
                if last_peak is not None:
                    self.store_cache[self.last_peak].is_peak = False
                cached.is_peak = True
                self.last_peak = header_hash
                return

        # We need to be in a sqlite transaction here.
        # Note: we do not commit this to the database yet, as we need to also change the coin store
        cursor_1 = await self.db.execute("UPDATE block_records SET is_peak=0 WHERE is_peak=1")
        await cursor_1.close()
        cursor_2 = await self.db.execute(
            "UPDATE block_records SET is_peak=1 WHERE header_hash=?",
            (header_hash.hex(),),
        )
        await cursor_2.close()

    async def is_fully_compactified(self, header_hash: bytes32) -> Optional[bool]:
        async with self.lock:
            cached = self.store_cache.get(header_hash, None)
            if cached is not None:
                return cached.block.is_fully_compactified()

        cursor = await self.db.execute(
            "SELECT is_fully_compactified from full_blocks WHERE header_hash=?", (header_hash.hex(),)
        )
        row = await cursor.fetchone()
        await cursor.close()
        if row is None:
            return None
        return bool(row[0])

    async def get_random_not_compactified(self, number: int) -> List[int]:
        # Since orphan blocks do not get compactified, we need to check whether all blocks with a
        # certain height are not compact. And if we do have compact orphan blocks, then all that
        # happens is that the occasional chain block stays uncompact - not ideal, but harmless.
        cursor = await self.db.execute(
            f"SELECT height FROM full_blocks GROUP BY height HAVING sum(is_fully_compactified)=0 "
            f"ORDER BY RANDOM() LIMIT {number}"
        )
        rows = await cursor.fetchall()
        await cursor.close()

        heights = []
        for row in rows:
            heights.append(int(row[0]))

        return heights
