"""持久化的全局顺序水位。

每次实体版本写入都在事务内从 SQLite 持久表 ``write_cursor`` 分配一个全局
单调递增水位，作为同一业务时刻（``valid_from`` 相同）多个版本的确定性
次序。水位完全由数据库状态决定：

* 不使用任何进程内计数，因此服务重启、多进程/多机器同时写入后顺序依然稳定；
* 分配发生在 ``BEGIN IMMEDIATE`` 写事务内，配合行锁保证并发提交拿到互不相同
  的水位；
* 幂等重试命中既有响应时不会再次调用 :func:`next_watermark`，因此重复请求
  不会生成新的水位。

旧数据库中迁移回填的版本使用非正水位（按提交次序从 0 向下编号），新写入
始终从 1 开始，二者在排序键 ``(valid_from, watermark)`` 上保持确定的全序。
"""

from __future__ import annotations

import sqlite3


def next_watermark(connection: sqlite3.Connection) -> int:
    """在当前事务内分配下一个全局水位。

    调用方必须已经持有写事务（``BEGIN IMMEDIATE``）。对单行的
    ``UPDATE ... SET value = value + 1`` 会在连接之间串行化，两个并发
    事务不可能读到同一个旧值。
    """
    connection.execute("UPDATE write_cursor SET value = value + 1 WHERE cursor_key = 'global'")
    row = connection.execute("SELECT value FROM write_cursor WHERE cursor_key = 'global'").fetchone()
    return int(row["value"])
