#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


import json
from unittest.mock import AsyncMock, patch

from click.testing import CliRunner, Result
from later.unittest import TestCase
from openr.py.openr.cli.clis import decision
from openr.py.openr.cli.commands.decision import DecisionValidateCmd, PathCmd
from openr.py.openr.cli.tests import helpers
from openr.thrift.OpenrCtrl.thrift_types import AdjacenciesFilter, OpenrError
from thrift.python.exceptions import (
    ApplicationError,
    ApplicationErrorType,
    TransportError,
    TransportErrorType,
)

from .fixtures import (
    AREA_SUMMARIES,
    DECISION_ADJ_DBS_MULTI_AREA,
    DECISION_ADJ_DBS_OK,
    EMPTY_KVSTORE_PUBLICATION,
    EMPTY_ROUTE_DB,
    EXPECTED_ROUTES_RECEIVED_JSON,
    EXPECTED_VALIDATE_OUTPUT_NO_PUBLISH,
    EXPECTED_VALIDATE_OUTPUT_OK,
    EXPECTED_VALIDATE_OUTPUT_STATIC_ROUTE_MISMATCH,
    EXPECTED_VALIDATE_OUTPUT_STATIC_ROUTE_UNSUPPORTED,
    KVSTORE_KEYVALS_OK,
    MOCKED_INIT_EVENTS_PASS,
    MOCKED_RECEIVED_ROUTES,
    RECEIVED_ROUTES_DB_OK,
    STATIC_ROUTES_MISMATCH,
    STATIC_ROUTES_OK,
)


BASE_MODULE: str = decision.__name__
BASE_CMD_MODULE = "openr.py.openr.cli.commands.decision"
NEIGHBORS_AREA_1 = "area1"
NEIGHBORS_AREA_2 = "area2"
NEIGHBORS_NODE = "abr-node"
NEIGHBORS_RIGHT_NODE = "openr-right"
NEIGHBORS_CENTER_NODE = "openr-center"
NEIGHBORS_FABRIC_ALIAS = "fabric-alias"
NEIGHBORS_LINK_METRIC_INCREMENT = 37
NEIGHBORS_RPC_ERROR_MESSAGE = "unknown method"
NEIGHBORS_JSON_ARGS = ["--json"]
NEIGHBORS_TEXT_HEADER = (
    "Neighbor    Local Intf    Remote Intf      Metric  "
    "NextHop-v4    NextHop-v6    Uptime    Area"
)
NEIGHBORS_COMPUTED_MULTI_AREA = {
    NEIGHBORS_AREA_1: [DECISION_ADJ_DBS_MULTI_AREA[0]],
    NEIGHBORS_AREA_2: [DECISION_ADJ_DBS_MULTI_AREA[1]],
}


class CliDecisionTests(TestCase):
    maxDiff: int | None = None

    def setUp(self) -> None:
        self.runner = CliRunner()

    def test_help(self) -> None:
        invoked_return = self.runner.invoke(
            decision.DecisionCli.decision,
            ["--help"],
            catch_exceptions=False,
        )
        self.assertEqual(0, invoked_return.exit_code)

    def _mock_neighbors_connection(self, mocked_openr_client: AsyncMock) -> AsyncMock:
        connection = helpers.get_enter_thrift_asyncmock(mocked_openr_client)
        connection.getDecisionAreaAdjacenciesComputed.return_value = (
            NEIGHBORS_COMPUTED_MULTI_AREA
        )
        connection.getDecisionAdjacenciesFiltered.return_value = (
            DECISION_ADJ_DBS_MULTI_AREA
        )
        return connection

    def _invoke_neighbors(self, args: list[str]) -> Result:
        return self.runner.invoke(
            decision.DecisionAdjCli.adj,
            ["--nodes", "all", *args],
            catch_exceptions=False,
        )

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_computed_all_areas(self, mocked_openr_client: AsyncMock) -> None:
        connection = self._mock_neighbors_connection(mocked_openr_client)

        result = self._invoke_neighbors(NEIGHBORS_JSON_ARGS)

        self.assertEqual(0, result.exit_code)
        output = json.loads(result.stdout)
        self.assertEqual({NEIGHBORS_AREA_1, NEIGHBORS_AREA_2}, set(output))
        self.assertIn(NEIGHBORS_NODE, output[NEIGHBORS_AREA_1])
        self.assertIn(NEIGHBORS_NODE, output[NEIGHBORS_AREA_2])
        self.assertEqual(
            "peer-area1",
            output[NEIGHBORS_AREA_1][NEIGHBORS_NODE]["adjacencies"][0]["otherNodeName"],
        )
        self.assertEqual(
            "peer-area2",
            output[NEIGHBORS_AREA_2][NEIGHBORS_NODE]["adjacencies"][0]["otherNodeName"],
        )
        connection.getDecisionAreaAdjacenciesComputed.assert_awaited_once_with(
            AdjacenciesFilter(selectAreas=set()), True
        )
        connection.getDecisionAdjacenciesFiltered.assert_not_awaited()

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_computed_forwards_area_filter(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        connection = self._mock_neighbors_connection(mocked_openr_client)
        connection.getDecisionAreaAdjacenciesComputed.return_value = {
            NEIGHBORS_AREA_2: NEIGHBORS_COMPUTED_MULTI_AREA[NEIGHBORS_AREA_2]
        }

        result = self._invoke_neighbors(
            [*NEIGHBORS_JSON_ARGS, "--areas", NEIGHBORS_AREA_2]
        )

        self.assertEqual(0, result.exit_code)
        self.assertEqual({NEIGHBORS_AREA_2}, set(json.loads(result.stdout)))
        connection.getDecisionAreaAdjacenciesComputed.assert_awaited_once_with(
            AdjacenciesFilter(selectAreas={NEIGHBORS_AREA_2}), True
        )
        connection.getDecisionAdjacenciesFiltered.assert_not_awaited()

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_computed_retains_node_filter(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        connection = self._mock_neighbors_connection(mocked_openr_client)
        connection.getDecisionAreaAdjacenciesComputed.return_value = {
            NEIGHBORS_AREA_2: [
                *DECISION_ADJ_DBS_OK,
                DECISION_ADJ_DBS_MULTI_AREA[1],
            ]
        }

        result = self.runner.invoke(
            decision.DecisionAdjCli.adj,
            ["--nodes", NEIGHBORS_NODE, *NEIGHBORS_JSON_ARGS],
            catch_exceptions=False,
        )

        self.assertEqual(0, result.exit_code)
        self.assertEqual(
            {NEIGHBORS_NODE}, set(json.loads(result.stdout)[NEIGHBORS_AREA_2])
        )
        connection.getDecisionAdjacenciesFiltered.assert_not_awaited()

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_computed_preserves_fabric_alias_and_link_metric_increment(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        connection = self._mock_neighbors_connection(mocked_openr_client)
        right_db, center_db = DECISION_ADJ_DBS_OK
        right_db = right_db(
            adjacencies=[
                right_db.adjacencies[0](
                    linkMetricIncrementVal=NEIGHBORS_LINK_METRIC_INCREMENT
                )
            ]
        )
        center_db = center_db(
            adjacencies=[center_db.adjacencies[0](otherIfName=NEIGHBORS_FABRIC_ALIAS)]
        )
        connection.getDecisionAreaAdjacenciesComputed.return_value = {
            NEIGHBORS_AREA_2: [right_db, center_db]
        }
        connection.getDecisionAdjacenciesFiltered.return_value = [right_db, center_db]

        result = self._invoke_neighbors(NEIGHBORS_JSON_ARGS)

        self.assertEqual(0, result.exit_code)
        output = json.loads(result.stdout)[NEIGHBORS_AREA_2]
        self.assertEqual({NEIGHBORS_RIGHT_NODE, NEIGHBORS_CENTER_NODE}, set(output))
        self.assertEqual(
            NEIGHBORS_LINK_METRIC_INCREMENT,
            output[NEIGHBORS_RIGHT_NODE]["adjacencies"][0]["linkMetricIncrementVal"],
        )
        self.assertEqual(
            NEIGHBORS_FABRIC_ALIAS,
            output[NEIGHBORS_CENTER_NODE]["adjacencies"][0]["otherIfName"],
        )
        connection.getDecisionAdjacenciesFiltered.assert_not_awaited()

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_computed_preserves_link_metric_increment(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        connection = self._mock_neighbors_connection(mocked_openr_client)
        right_db, center_db = DECISION_ADJ_DBS_OK
        databases = [
            right_db(
                adjacencies=[
                    right_db.adjacencies[0](
                        linkMetricIncrementVal=NEIGHBORS_LINK_METRIC_INCREMENT
                    )
                ]
            ),
            center_db,
        ]
        connection.getDecisionAreaAdjacenciesComputed.return_value = {
            NEIGHBORS_AREA_2: databases
        }
        connection.getDecisionAdjacenciesFiltered.return_value = databases

        result = self._invoke_neighbors(NEIGHBORS_JSON_ARGS)

        self.assertEqual(0, result.exit_code)
        self.assertEqual(
            NEIGHBORS_LINK_METRIC_INCREMENT,
            json.loads(result.stdout)[NEIGHBORS_AREA_2][NEIGHBORS_RIGHT_NODE][
                "adjacencies"
            ][0]["linkMetricIncrementVal"],
        )

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_computed_text_output(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        connection = self._mock_neighbors_connection(mocked_openr_client)

        result = self._invoke_neighbors([])

        self.assertEqual(0, result.exit_code)
        self.assertEqual(
            [
                "",
                "== Area: area1 ==",
                "",
                "> abr-node   ",
                NEIGHBORS_TEXT_HEADER,
                "peer-area1  if-area1      peer-if-1             1  "
                "10.0.0.1      fe80::1                 area1",
                "",
                "",
                "",
                "== Area: area2 ==",
                "",
                "> abr-node   ",
                NEIGHBORS_TEXT_HEADER,
                "peer-area2  if-area2      peer-if-2             1  "
                "10.0.0.2      fe80::2                 area2",
                "",
                "",
            ],
            result.stdout.splitlines(),
        )
        connection.getDecisionAdjacenciesFiltered.assert_not_awaited()

        connection.getDecisionAreaAdjacenciesComputed.assert_awaited_once_with(
            AdjacenciesFilter(selectAreas=set()), True
        )
        connection.getDecisionAreaAdjacenciesComputed.reset_mock()
        raw_result = self._invoke_neighbors(["--no-bidir"])

        self.assertEqual(0, raw_result.exit_code)
        self.assertEqual(result.stdout, raw_result.stdout)
        connection.getDecisionAreaAdjacenciesComputed.assert_awaited_once_with(
            AdjacenciesFilter(selectAreas=set()), False
        )
        connection.getDecisionAdjacenciesFiltered.assert_not_awaited()

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_no_bidir_uses_raw(self, mocked_openr_client: AsyncMock) -> None:
        connection = self._mock_neighbors_connection(mocked_openr_client)
        connection.getDecisionAreaAdjacenciesComputed.return_value = {
            NEIGHBORS_AREA_1: NEIGHBORS_COMPUTED_MULTI_AREA[NEIGHBORS_AREA_1]
        }
        connection.getDecisionAdjacenciesFiltered.return_value = (
            NEIGHBORS_COMPUTED_MULTI_AREA[NEIGHBORS_AREA_1]
        )

        result = self._invoke_neighbors(
            [*NEIGHBORS_JSON_ARGS, "--no-bidir", "--areas", NEIGHBORS_AREA_1]
        )

        self.assertEqual(0, result.exit_code)
        output = json.loads(result.stdout)
        self.assertEqual({NEIGHBORS_AREA_1}, set(output))
        self.assertIn(NEIGHBORS_NODE, output[NEIGHBORS_AREA_1])
        connection.getDecisionAreaAdjacenciesComputed.assert_awaited_once_with(
            AdjacenciesFilter(selectAreas={NEIGHBORS_AREA_1}), False
        )
        connection.getDecisionAdjacenciesFiltered.assert_not_awaited()

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_unknown_method_falls_back_with_legacy_filter(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        connection = self._mock_neighbors_connection(mocked_openr_client)
        connection.getDecisionAreaAdjacenciesComputed.side_effect = ApplicationError(
            ApplicationErrorType.UNKNOWN_METHOD, NEIGHBORS_RPC_ERROR_MESSAGE
        )
        connection.getDecisionAdjacenciesFiltered.return_value = [
            *DECISION_ADJ_DBS_OK,
            DECISION_ADJ_DBS_MULTI_AREA[1],
        ]

        result = self._invoke_neighbors(
            [*NEIGHBORS_JSON_ARGS, "--areas", NEIGHBORS_AREA_2]
        )

        self.assertEqual(0, result.exit_code)
        self.assertEqual(
            {NEIGHBORS_RIGHT_NODE, NEIGHBORS_CENTER_NODE},
            set(json.loads(result.stdout)[NEIGHBORS_AREA_2]),
        )
        expected_filter = AdjacenciesFilter(selectAreas={NEIGHBORS_AREA_2})
        connection.getDecisionAreaAdjacenciesComputed.assert_awaited_once_with(
            expected_filter, True
        )
        connection.getDecisionAdjacenciesFiltered.assert_awaited_once_with(
            expected_filter
        )

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_no_bidir_unknown_method_keeps_raw_adjacencies(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        connection = self._mock_neighbors_connection(mocked_openr_client)
        connection.getDecisionAreaAdjacenciesComputed.side_effect = ApplicationError(
            ApplicationErrorType.UNKNOWN_METHOD, NEIGHBORS_RPC_ERROR_MESSAGE
        )
        connection.getDecisionAdjacenciesFiltered.return_value = (
            NEIGHBORS_COMPUTED_MULTI_AREA[NEIGHBORS_AREA_1]
        )

        result = self._invoke_neighbors(
            [*NEIGHBORS_JSON_ARGS, "--no-bidir", "--areas", NEIGHBORS_AREA_1]
        )

        self.assertEqual(0, result.exit_code)
        output = json.loads(result.stdout)
        self.assertEqual({NEIGHBORS_AREA_1}, set(output))
        self.assertEqual({NEIGHBORS_NODE}, set(output[NEIGHBORS_AREA_1]))
        expected_filter = AdjacenciesFilter(selectAreas={NEIGHBORS_AREA_1})
        connection.getDecisionAreaAdjacenciesComputed.assert_awaited_once_with(
            expected_filter, False
        )
        connection.getDecisionAdjacenciesFiltered.assert_awaited_once_with(
            expected_filter
        )

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_other_application_errors_propagate(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        for error_type in ApplicationErrorType:
            if error_type == ApplicationErrorType.UNKNOWN_METHOD:
                continue
            with self.subTest(error_type=error_type):
                connection = self._mock_neighbors_connection(mocked_openr_client)
                error = ApplicationError(error_type, NEIGHBORS_RPC_ERROR_MESSAGE)
                connection.getDecisionAreaAdjacenciesComputed.side_effect = error

                with self.assertRaises(ApplicationError) as raised:
                    self._invoke_neighbors(NEIGHBORS_JSON_ARGS)

                self.assertIs(error, raised.exception)
                connection.getDecisionAdjacenciesFiltered.assert_not_awaited()

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_neighbors_transport_and_server_errors_propagate(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        for error in (
            TransportError(
                TransportErrorType.TIMED_OUT, NEIGHBORS_RPC_ERROR_MESSAGE, 0, 0
            ),
            OpenrError(message=NEIGHBORS_RPC_ERROR_MESSAGE),
        ):
            with self.subTest(error=error):
                connection = self._mock_neighbors_connection(mocked_openr_client)
                connection.getDecisionAreaAdjacenciesComputed.side_effect = error

                with self.assertRaises(type(error)) as raised:
                    self._invoke_neighbors(NEIGHBORS_JSON_ARGS)

                self.assertIs(error, raised.exception)
                connection.getDecisionAdjacenciesFiltered.assert_not_awaited()

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_decision_validate_all_areas(self, mocked_openr_client: AsyncMock) -> None:
        mocked_returned_connection = helpers.get_enter_thrift_asyncmock(
            mocked_openr_client
        )
        # Have some Areas Returned
        mocked_returned_connection.getKvStoreAreaSummary.return_value = AREA_SUMMARIES
        # Have decision adjacencies returned
        mocked_returned_connection.getDecisionAdjacenciesFiltered.return_value = (
            DECISION_ADJ_DBS_OK
        )
        # Have routes returned
        mocked_returned_connection.getReceivedRoutesFiltered.return_value = (
            RECEIVED_ROUTES_DB_OK
        )
        # Have kvstore data returned
        mocked_returned_connection.getKvStoreKeyValsFilteredArea.return_value = (
            KVSTORE_KEYVALS_OK
        )

        # Have published initialization events returned
        mocked_returned_connection.getInitializationEvents.return_value = (
            MOCKED_INIT_EVENTS_PASS
        )
        # Have matching static routes returned
        mocked_returned_connection.getPrefixMgrPublishedStaticRoutes.return_value = (
            STATIC_ROUTES_OK
        )
        mocked_returned_connection.getDecisionStaticRoutes.return_value = (
            STATIC_ROUTES_OK
        )

        with patch("openr.py.openr.cli.utils.utils.get_area_id", return_value=69):
            invoked_return = self.runner.invoke(
                decision.DecisionValidateCli.validate,
                [],  # No args
                catch_exceptions=False,
            )
        self.assertEqual(0, invoked_return.exit_code)
        self.assertEqual(EXPECTED_VALIDATE_OUTPUT_OK, invoked_return.stdout)

        # Test bad - initialization event not published
        mocked_returned_connection.getInitializationEvents.return_value = {}

        with patch("openr.py.openr.cli.utils.utils.get_area_id", return_value=69):
            invoked_return = self.runner.invoke(
                decision.DecisionValidateCli.validate,
                [],  # No args
                catch_exceptions=False,
            )
        self.assertEqual(1, invoked_return.exit_code)
        self.assertEqual(EXPECTED_VALIDATE_OUTPUT_NO_PUBLISH, invoked_return.stdout)

    def _mock_validate_connection(self, mocked_openr_client: AsyncMock) -> AsyncMock:
        """Mocks a node where every decision validate check passes."""
        connection = helpers.get_enter_thrift_asyncmock(mocked_openr_client)
        connection.getKvStoreAreaSummary.return_value = AREA_SUMMARIES
        connection.getDecisionAdjacenciesFiltered.return_value = DECISION_ADJ_DBS_OK
        connection.getReceivedRoutesFiltered.return_value = RECEIVED_ROUTES_DB_OK
        connection.getKvStoreKeyValsFilteredArea.return_value = KVSTORE_KEYVALS_OK
        connection.getInitializationEvents.return_value = MOCKED_INIT_EVENTS_PASS
        connection.getPrefixMgrPublishedStaticRoutes.return_value = STATIC_ROUTES_OK
        connection.getDecisionStaticRoutes.return_value = STATIC_ROUTES_OK
        return connection

    def _invoke_validate(self) -> Result:
        with (
            patch("openr.py.openr.cli.utils.utils.get_area_id", return_value=69),
            patch.object(DecisionValidateCmd, "STATIC_ROUTE_CHECK_RETRY_INTERVAL_S", 0),
        ):
            return self.runner.invoke(
                decision.DecisionValidateCli.validate,
                [],
                catch_exceptions=False,
            )

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_decision_validate_static_route_mismatch(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        connection = self._mock_validate_connection(mocked_openr_client)
        connection.getDecisionStaticRoutes.return_value = STATIC_ROUTES_MISMATCH

        invoked_return = self._invoke_validate()

        self.assertEqual(1, invoked_return.exit_code)
        self.assertEqual(
            EXPECTED_VALIDATE_OUTPUT_STATIC_ROUTE_MISMATCH, invoked_return.stdout
        )
        self.assertEqual(
            DecisionValidateCmd.STATIC_ROUTE_CHECK_ATTEMPTS,
            connection.getDecisionStaticRoutes.await_count,
        )

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_decision_validate_static_route_transient_mismatch(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        """A mismatch that clears on retry, e.g. Decision catching up, passes."""
        connection = self._mock_validate_connection(mocked_openr_client)
        connection.getDecisionStaticRoutes.side_effect = [
            STATIC_ROUTES_MISMATCH,
            STATIC_ROUTES_OK,
        ]

        invoked_return = self._invoke_validate()

        self.assertEqual(0, invoked_return.exit_code)
        self.assertEqual(EXPECTED_VALIDATE_OUTPUT_OK, invoked_return.stdout)
        self.assertEqual(2, connection.getDecisionStaticRoutes.await_count)

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_decision_validate_static_route_unsupported(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        """An Open/R binary without the static route APIs skips the check."""
        connection = self._mock_validate_connection(mocked_openr_client)
        connection.getPrefixMgrPublishedStaticRoutes.side_effect = ApplicationError(
            ApplicationErrorType.UNKNOWN_METHOD, "unknown method"
        )

        invoked_return = self._invoke_validate()

        self.assertEqual(0, invoked_return.exit_code)
        self.assertEqual(
            EXPECTED_VALIDATE_OUTPUT_STATIC_ROUTE_UNSUPPORTED, invoked_return.stdout
        )

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_decision_validate_static_route_other_error_raises(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        """Only a missing API is skipped; other server errors still surface."""
        connection = self._mock_validate_connection(mocked_openr_client)
        connection.getDecisionStaticRoutes.side_effect = ApplicationError(
            ApplicationErrorType.INTERNAL_ERROR, "boom"
        )

        with self.assertRaises(ApplicationError):
            self._invoke_validate()

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_decision_received_routes_json(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        mocked_returned_connection = helpers.get_enter_thrift_asyncmock(
            mocked_openr_client
        )
        # Retturn a List of ReceivedRouteDetail
        mocked_returned_connection.getReceivedRoutesFiltered.return_value = (
            MOCKED_RECEIVED_ROUTES
        )
        invoked_return = self.runner.invoke(
            decision.ReceivedRoutesCli.show,
            ["--json"],
            catch_exceptions=False,
        )
        self.assertEqual(0, invoked_return.exit_code)
        self.assertEqual(EXPECTED_ROUTES_RECEIVED_JSON, invoked_return.stdout)

    def test_path_get_if2node_map_merges_multi_area(self) -> None:
        """On a multi-area (ABR) node, the same thisNodeName appears once per
        area. get_if2node_map must merge all per-area adjacencies into a single
        per-node nexthop dict instead of overwriting (the old iter_dbs path
        filtered to a single area)."""
        if2node = PathCmd().get_if2node_map(DECISION_ADJ_DBS_MULTI_AREA)

        self.assertIn("abr-node", if2node)
        # Both per-area adjacencies for "abr-node" must survive the merge.
        nexthop_dict = if2node["abr-node"]
        self.assertEqual(
            "peer-area1",
            nexthop_dict[("if-area1", "fe80::1")],
        )
        self.assertEqual(
            "peer-area2",
            nexthop_dict[("if-area2", "fe80::2")],
        )

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_decision_path_with_area(self, mocked_openr_client: AsyncMock) -> None:
        """`breeze decision path --area <area> ...` must use the area-aware
        adjacency API restricted to the given area (T267753415)."""
        mocked_returned_connection = helpers.get_enter_thrift_asyncmock(
            mocked_openr_client
        )
        mocked_returned_connection.getDecisionAreaAdjacenciesFiltered.return_value = {}
        mocked_returned_connection.getKvStoreKeyValsFilteredArea.return_value = (
            EMPTY_KVSTORE_PUBLICATION
        )
        mocked_returned_connection.getRouteDbComputed.return_value = EMPTY_ROUTE_DB

        with patch(
            "openr.py.openr.cli.utils.utils.get_area_id",
            new=AsyncMock(return_value="rb_rb_plane002"),
        ):
            invoked_return = self.runner.invoke(
                decision.PathCli.path,
                [
                    "--area",
                    "rb_rb_plane002",
                    "--src",
                    "src-node",
                    "--dst",
                    "fdad:ff01:31e::d:0/128",
                ],
                catch_exceptions=False,
            )

        self.assertEqual(0, invoked_return.exit_code)

        # The fix replaces getDecisionAdjacencyDbs() (single-area, throws on
        # multi-area configs) and the older getDecisionAdjacenciesFiltered()
        # with the area-keyed getDecisionAreaAdjacenciesFiltered.
        mocked_returned_connection.getDecisionAdjacencyDbs.assert_not_awaited()
        mocked_returned_connection.getDecisionAdjacenciesFiltered.assert_not_awaited()
        mocked_returned_connection.getDecisionAreaAdjacenciesFiltered.assert_awaited_once()
        (filter_arg,) = (
            mocked_returned_connection.getDecisionAreaAdjacenciesFiltered.await_args.args
        )
        self.assertEqual({"rb_rb_plane002"}, set(filter_arg.selectAreas))

        # KvStore prefix dump is restricted to the requested area.
        mocked_returned_connection.getKvStoreKeyValsFilteredArea.assert_awaited_once()
        kv_call = mocked_returned_connection.getKvStoreKeyValsFilteredArea.await_args
        self.assertEqual("rb_rb_plane002", kv_call.args[1])

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_decision_path_without_area(self, mocked_openr_client: AsyncMock) -> None:
        """`--area` is optional; when omitted, `get_area_id` resolves the
        default (the single configured area on a single-area node, or an error
        on multi-area nodes). The thrift call must still go through the
        area-aware adjacency API with the resolved area."""
        mocked_returned_connection = helpers.get_enter_thrift_asyncmock(
            mocked_openr_client
        )
        mocked_returned_connection.getDecisionAreaAdjacenciesFiltered.return_value = {}
        mocked_returned_connection.getKvStoreKeyValsFilteredArea.return_value = (
            EMPTY_KVSTORE_PUBLICATION
        )
        mocked_returned_connection.getRouteDbComputed.return_value = EMPTY_ROUTE_DB

        with patch(
            "openr.py.openr.cli.utils.utils.get_area_id",
            new=AsyncMock(return_value="default-area"),
        ):
            invoked_return = self.runner.invoke(
                decision.PathCli.path,
                ["--src", "src-node", "--dst", "fdad:ff01:31e::d:0/128"],
                catch_exceptions=False,
            )

        self.assertEqual(0, invoked_return.exit_code)
        mocked_returned_connection.getDecisionAreaAdjacenciesFiltered.assert_awaited_once()
        (filter_arg,) = (
            mocked_returned_connection.getDecisionAreaAdjacenciesFiltered.await_args.args
        )
        self.assertEqual({"default-area"}, set(filter_arg.selectAreas))

    @patch(helpers.COMMANDS_GET_OPENR_CTRL_CPP_CLIENT)
    def test_decision_received_routes_json_no_data(
        self, mocked_openr_client: AsyncMock
    ) -> None:
        mocked_returned_connection = helpers.get_enter_thrift_asyncmock(
            mocked_openr_client
        )
        # Retturn a List of ReceivedRouteDetail
        mocked_returned_connection.getReceivedRoutesFiltered.return_value = []
        invoked_return = self.runner.invoke(
            decision.ReceivedRoutesCli.show,
            ["--json"],
            catch_exceptions=False,
        )
        self.assertEqual(0, invoked_return.exit_code)  # TODO - Should we return 1?
        self.assertEqual("[]\n", invoked_return.stdout)
