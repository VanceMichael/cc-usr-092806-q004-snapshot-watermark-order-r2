"""持久化全局顺序水位分配。

水位 ``seq`` 存储在 ``sequence_watermarks`` 单行表中，在调用方的
IMMEDIATE 事务内通过条件更新原子地自增。它因此：

* 单调递增且持久化——重启后从磁盘读回，跨机器重放也由提交顺序决定；
* 全局唯一——所有实体版本共用同一水位空间，可跨案件比较先后；
* 不依赖任何进程内计数器——不存在"重启后从头计数"的问题。
"""

from __future__ import annotations

import sqlite3


def allocate(connection: sqlite3.Connection) -> int:
    """在当前事务内分配下一个顺序水位并返回。"""
    connection.execute(
        "UPDATE sequence_watermarks SET last_seq = last_seq + 1 WHERE singleton = 1"
    )
    row = connection.execute("SELECT last_seq FROM sequence_watermarks WHERE singleton = 1").fetchone()
    return int(row["last_seq"])


def high_watermark(connection: sqlite3.Connection) -> int:
    """返回当前已经持久化的最高水位（无写入时为 0）。"""
    row = connection.execute("SELECT last_seq FROM sequence_watermarks WHERE singleton = 1").fetchone()
    return int(row["last_seq"]) if row else 0
