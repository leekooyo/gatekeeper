"""Run the real plugin class with only AstrBot framework boundaries replaced."""
import ast
import asyncio
import json
import random
import unittest
import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

SOURCE = Path(__file__).resolve().parents[1] / 'main.py'
tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef, ast.Assign))]
for n in nodes:
    if isinstance(n, ast.ClassDef):
        for fn in n.body:
            if isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                fn.decorator_list = [d for d in fn.decorator_list if isinstance(d, ast.Name) and d.id in ('staticmethod', 'classmethod')]
namespace = dict(asyncio=asyncio, json=json, random=random, datetime=datetime, Path=Path,
                 Star=object, star_map={}, logger=SimpleNamespace(**{k: lambda *a: None for k in ('info','warning','error')}))
code = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)] + nodes, type_ignores=[])
exec(compile(ast.fix_missing_locations(code), str(SOURCE), 'exec'), namespace)
Plugin = namespace['GatekeeperPlugin']


class GatekeeperTests(unittest.IsolatedAsyncioTestCase):
    def make_plugin(self, response=None, failure=None):
        p = Plugin.__new__(Plugin)
        p.config = {'enable': True}
        p._umo_prefix = ''
        p._blocks = []
        p._read_policy = lambda: {'groups': {'1': {'mode': 'plugin', 'manual_off': []},
                                            '9': {'mode': 'both', 'manual_off': []}}, 'meta': {'released': False}}
        bot = SimpleNamespace(get_group_list=AsyncMock(return_value=response, side_effect=failure))
        adapter = SimpleNamespace(meta=lambda: SimpleNamespace(id='qq', name='aiocqhttp'), get_client=lambda: bot)
        p.context = SimpleNamespace(platform_manager=SimpleNamespace(platform_insts=[adapter]))
        p._safe_maintain = AsyncMock()
        return p, bot

    async def test_state_distinguishes_joined_configured_and_stale(self):
        p, bot = self.make_plugin([{'group_id': 1, 'group_name': 'A'}, {'group_id': 2, 'group_name': 'B'}])
        state = await p.api_state()
        self.assertEqual(state['joined_count'], 2)
        self.assertEqual(state['configured_count'], 2)
        self.assertEqual(state['unconfigured_count'], 1)
        self.assertEqual(state['stale_count'], 1)
        rows = {g['gid']: g for g in state['groups']}
        self.assertFalse(rows['2']['registered'])
        self.assertEqual(rows['2']['mode'], 'mute')
        self.assertFalse(rows['9']['joined'])
        bot.get_group_list.assert_awaited_once()
        self.assertEqual(p._all_gids(), ['1', '9'], 'inventory must never silently admit groups')

    async def test_offline_does_not_claim_zero_joined_groups(self):
        p, _ = self.make_plugin(failure=RuntimeError('offline'))
        state = await p.api_state()
        self.assertIsNone(state['joined_count'])
        self.assertTrue(state['inventory_error'])
        self.assertEqual(len(state['groups']), 2)
        self.assertTrue(all(g['joined'] is None for g in state['groups']))

    async def test_gate_list_reports_both_counts(self):
        p, _ = self.make_plugin([{'group_id': i, 'group_name': str(i)} for i in range(1, 4)])
        event = SimpleNamespace(message_str='/gate list', is_admin=lambda: True, get_group_id=lambda: '',
                                get_platform_id=lambda: 'qq', plain_result=lambda text: text)
        result = [r async for r in p.gate_cmd(event)]
        self.assertIn('实际加入 3 群', result[0])
        self.assertIn('已配置 2 群', result[0])
        self.assertIn('当前群列表 · 共 3 个', result[0])
        self.assertIn('默认静默（未配置）', result[0])
        self.assertNotIn('群 9｜', result[0])

    async def test_unknown_group_stops_without_persistent_projection(self):
        p, _ = self.make_plugin([])
        event = SimpleNamespace(get_platform_id=lambda: 'qq', get_group_id=lambda: '100',
                                get_extra=lambda key: [], get_messages=lambda: [],
                                stop_event=lambda: setattr(event, 'stopped', True))
        await p.gate(event)
        self.assertTrue(event.stopped)
        p._safe_maintain.assert_not_awaited()

    async def test_plugin_mode_blocks_default_llm(self):
        p, _ = self.make_plugin([])
        calls = []
        event = SimpleNamespace(get_platform_id=lambda: 'qq', get_group_id=lambda: '1',
                                get_extra=lambda key: [], plugins_name=['meme_manager'],
                                should_call_llm=lambda stop: calls.append(stop))
        await p.gate(event)
        self.assertEqual(calls, [True])
        p._safe_maintain.assert_awaited_once()

    async def test_mode_all_uses_current_qq_membership_on_each_command(self):
        p, bot = self.make_plugin([{'group_id': 1}, {'group_id': 2}, {'group_id': 3}])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'modes.json'
            path.write_text(json.dumps({'version': 1, 'groups': {
                '1': {'mode': 'both', 'manual_off': []},
                '9': {'mode': 'mute', 'manual_off': []}}, 'meta': {'released': False}}), encoding='utf-8')
            p._policy_path = lambda: path
            del p._read_policy
            p._reapply_many = AsyncMock(return_value={'groups': 3, 'projected': 3, 'failed': []})
            event = SimpleNamespace(message_str='/gate mode all plugin', is_admin=lambda: True,
                                    get_group_id=lambda: '', get_platform_id=lambda: 'qq',
                                    plain_result=lambda text: text)
            first = [x async for x in p.gate_cmd(event)][0]
            self.assertIn('3 个', first)
            policy = json.loads(path.read_text(encoding='utf-8'))
            self.assertEqual({k for k, v in policy['groups'].items() if v['mode'] == 'plugin'}, {'1','2','3'})
            self.assertEqual(policy['groups']['9']['mode'], 'mute')
            bot.get_group_list.return_value = [{'group_id': 1}, {'group_id': 3}, {'group_id': 4}]
            second = [x async for x in p.gate_cmd(event)][0]
            self.assertIn('3 个', second)
            policy = json.loads(path.read_text(encoding='utf-8'))
            self.assertEqual(policy['groups']['4']['mode'], 'plugin')
            self.assertEqual(policy['groups']['2']['mode'], 'plugin', 'old policy stays until explicitly removed')
            self.assertEqual(bot.get_group_list.await_count, 2)

    async def test_mode_all_refuses_to_write_when_qq_inventory_is_unavailable(self):
        p, _ = self.make_plugin(failure=RuntimeError('offline'))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'modes.json'
            path.write_text(json.dumps({'version': 1, 'groups': {'1': {'mode': 'both', 'manual_off': []}},
                                        'meta': {'released': False}}), encoding='utf-8')
            before = path.read_bytes()
            p._policy_path = lambda: path
            event = SimpleNamespace(message_str='/gate mode all plugin', is_admin=lambda: True,
                                    get_group_id=lambda: '', get_platform_id=lambda: 'qq',
                                    plain_result=lambda text: text)
            answer = [x async for x in p.gate_cmd(event)][0]
            self.assertIn('读取失败', answer)
            self.assertEqual(path.read_bytes(), before)

    async def test_projection_reports_failed_group_and_continues(self):
        p, _ = self.make_plugin([])
        p._read_state = lambda: {'backup': {}}
        p._lock = asyncio.Lock()
        p._umos_of_gid = lambda gid, backup: [f'qq:GroupMessage:{gid}']
        async def project(umo, mode, off):
            if umo.endswith(':1'):
                raise RuntimeError('disk error')
        p._project = AsyncMock(side_effect=project)
        result = await p._reapply_many(['1', '9'])
        self.assertEqual(result, {'groups': 1, 'projected': 1, 'failed': ['1']})
        self.assertEqual(p._project.await_count, 2)


if __name__ == '__main__':
    unittest.main()
