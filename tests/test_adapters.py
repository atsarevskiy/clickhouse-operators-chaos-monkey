"""Offline checks of the adapters and the registry. No cluster needed: python3 -m unittest"""

from __future__ import annotations

import unittest

from chaosmonkey import scenarios
from chaosmonkey.cluster import K3dCluster
from chaosmonkey.model import ClusterSpec, ScenarioResult
from chaosmonkey.operators import ADAPTERS


def adapters():
    cluster = K3dCluster("offline")
    return [cls(cluster, "0.0.0") for cls in ADAPTERS.values()]


class RenderTest(unittest.TestCase):
    def test_every_adapter_renders_topology(self):
        spec = ClusterSpec(shards=3, replicas=2, keepers=3)
        for op in adapters():
            with self.subTest(op=op.name):
                objs = op.render(spec)
                self.assertEqual(len(objs), 2)
                text = repr(objs)
                self.assertIn("3", text)
                for obj in objs:
                    self.assertEqual(obj["metadata"]["namespace"], spec.namespace)

    def test_workload_user_is_provisioned(self):
        for op in adapters():
            with self.subTest(op=op.name):
                self.assertIn(op.workload_user, repr(op.render(ClusterSpec())))

    def test_spec_knobs_reach_the_resources(self):
        spec = ClusterSpec(pod_annotations={"chaosmonkey/roll": "42"}, init_sleep=5, termination_grace=77)
        for op in adapters():
            with self.subTest(op=op.name):
                text = repr(op.render(spec))
                self.assertIn("'chaosmonkey/roll': '42'", text)
                self.assertIn("77", text)
                self.assertIn("slow-start", text)

    def test_prestop_only_where_supported(self):
        spec = ClusterSpec(prestop_sleep=900)
        for op in adapters():
            with self.subTest(op=op.name):
                has = "preStop" in repr(op.render(spec))
                self.assertEqual(has, "prestop" in op.capabilities)

    def test_selectors_scope_to_shard_and_replica(self):
        spec = ClusterSpec()
        for op in adapters():
            with self.subTest(op=op.name):
                base, one = op.server_selector(spec), op.server_selector(spec, 1, 0)
                self.assertTrue(one.startswith(base))
                self.assertGreater(one.count(","), base.count(","))
                self.assertEqual(len(op.keeper_hosts(spec)), spec.keepers)


class RegistryTest(unittest.TestCase):
    def test_profiles_reference_known_scenarios(self):
        for name, ids in scenarios.PROFILES.items():
            with self.subTest(profile=name):
                self.assertEqual([s.id for s in scenarios.select(name, None)], ids)

    def test_ids_unique(self):
        ids = [s.id for s in scenarios.ALL]
        self.assertEqual(len(ids), len(set(ids)))

    def test_requirements_are_known_capabilities(self):
        known = set().union(*(op.capabilities for op in adapters())) | {"prestop", "volume-recreate", "pod-labels"}
        for s in scenarios.ALL:
            self.assertLessEqual(set(s.requires), known, s.id)


class VerdictTest(unittest.TestCase):
    def test_worst_finding_decides(self):
        r = ScenarioResult("x", "pods", "op", "1")
        r.decide()
        self.assertEqual(r.verdict, "PASS")
        r.add("warn", "a", "b")
        r.decide()
        self.assertEqual(r.verdict, "DEGRADED")
        r.add("fail", "a", "b")
        r.decide()
        self.assertEqual(r.verdict, "FAIL")


if __name__ == "__main__":
    unittest.main()
