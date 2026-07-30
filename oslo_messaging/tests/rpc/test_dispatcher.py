# Copyright 2013 Red Hat, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import testscenarios
import time

import oslo_messaging
from oslo_messaging import rpc
from oslo_messaging import serializer as msg_serializer
from oslo_messaging.tests import utils as test_utils
from unittest import mock

load_tests = testscenarios.load_tests_apply_scenarios


class _FakeEndpoint:
    def __init__(self, target=None):
        self.target = target

    def foo(self, ctxt, **kwargs):
        pass

    @rpc.expose
    def bar(self, ctxt, **kwargs):
        pass

    def _foobar(self, ctxt, **kwargs):
        pass


class TestDispatcher(test_utils.BaseTestCase):
    scenarios = [
        ('no_endpoints',
         dict(endpoints=[],
              access_policy=None,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo'),
              exposed_methods=['foo', 'bar', '_foobar'],
              success=False, ex=oslo_messaging.UnsupportedVersion)),
        ('default_target',
         dict(endpoints=[{}],
              access_policy=None,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo'),
              exposed_methods=['foo', 'bar', '_foobar'],
              success=True, ex=None)),
        ('default_target_ctxt_and_args',
         dict(endpoints=[{}],
              access_policy=oslo_messaging.LegacyRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='bar'),
              ctxt=dict(user='bob'), msg=dict(method='bar',
                                              args=dict(blaa=True)),
              exposed_methods=['foo', 'bar', '_foobar'],
              success=True, ex=None)),
        ('default_target_namespace',
         dict(endpoints=[{}],
              access_policy=oslo_messaging.LegacyRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace=None),
              exposed_methods=['foo', 'bar', '_foobar'],
              success=True, ex=None)),
        ('default_target_version',
         dict(endpoints=[{}],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', version='1.0'),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('default_target_no_such_method',
         dict(endpoints=[{}],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foobar'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.NoSuchMethod)),
        ('namespace',
         dict(endpoints=[{}, dict(namespace='testns')],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=1, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace='testns'),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('namespace_mismatch',
         dict(endpoints=[{}, dict(namespace='testns')],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo', namespace='nstest'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.UnsupportedVersion)),
        ('version',
         dict(endpoints=[dict(version='1.5'), dict(version='3.4')],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=1, method='foo'),
              ctxt={}, msg=dict(method='foo', version='3.2'),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('version_mismatch',
         dict(endpoints=[dict(version='1.5'), dict(version='3.0')],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo', version='3.2'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.UnsupportedVersion)),
        ('message_in_null_namespace_with_multiple_namespaces',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=[None])],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace=None),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('message_in_wrong_namespace_with_multiple_namespaces',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None])],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=None,
              ctxt={}, msg=dict(method='foo', namespace='wrong'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.UnsupportedVersion)),
        ('message_with_endpoint_no_private_and_public_method',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None])],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace='testns'),
              exposed_methods=['foo', 'bar'],
              success=True, ex=None)),
        ('message_with_endpoint_no_private_and_private_method',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None], )],
              access_policy=oslo_messaging.DefaultRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='_foobar'),
              ctxt={}, msg=dict(method='_foobar', namespace='testns'),
              exposed_methods=['foo', 'bar'],
              success=False, ex=oslo_messaging.NoSuchMethod)),
        ('message_with_endpoint_explicitly_exposed_without_exposed_method',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None], )],
              access_policy=oslo_messaging.ExplicitRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='foo'),
              ctxt={}, msg=dict(method='foo', namespace='testns'),
              exposed_methods=['bar'],
              success=False, ex=oslo_messaging.NoSuchMethod)),
        ('message_with_endpoint_explicitly_exposed_with_exposed_method',
         dict(endpoints=[dict(namespace='testns',
                              legacy_namespaces=['second', None], )],
              access_policy=oslo_messaging.ExplicitRPCAccessPolicy,
              dispatch_to=dict(endpoint=0, method='bar'),
              ctxt={}, msg=dict(method='bar', namespace='testns'),
              exposed_methods=['bar'],
              success=True, ex=None)),
    ]

    def test_dispatcher(self):

        def _set_endpoint_mock_properties(endpoint):
            endpoint.foo = mock.Mock(spec=dir(_FakeEndpoint.foo))
            # mock doesn't pick up the decorated method.
            endpoint.bar = mock.Mock(spec=dir(_FakeEndpoint.bar))
            endpoint.bar.exposed = mock.PropertyMock(return_value=True)
            endpoint._foobar = mock.Mock(spec=dir(_FakeEndpoint._foobar))

            return endpoint

        endpoints = [_set_endpoint_mock_properties(mock.Mock(
            spec=_FakeEndpoint, target=oslo_messaging.Target(**e)))
            for e in self.endpoints]

        serializer = None
        dispatcher = oslo_messaging.RPCDispatcher(endpoints, serializer,
                                                  self.access_policy)

        incoming = mock.Mock(ctxt=self.ctxt, message=self.msg,
                             client_timeout=0)

        res = None

        try:
            res = dispatcher.dispatch(incoming)
        except Exception as ex:
            self.assertFalse(self.success, ex)
            self.assertIsNotNone(self.ex, ex)
            self.assertIsInstance(ex, self.ex, ex)
            if isinstance(ex, oslo_messaging.NoSuchMethod):
                self.assertEqual(self.msg.get('method'), ex.method)
            elif isinstance(ex, oslo_messaging.UnsupportedVersion):
                self.assertEqual(self.msg.get('version', '1.0'),
                                 ex.version)
                if ex.method:
                    self.assertEqual(self.msg.get('method'), ex.method)
        else:
            self.assertTrue(self.success,
                            "Unexpected success of operation during testing")
            self.assertIsNotNone(res)

        for n, endpoint in enumerate(endpoints):
            for method_name in self.exposed_methods:
                method = getattr(endpoint, method_name)
                if self.dispatch_to and n == self.dispatch_to['endpoint'] and \
                        method_name == self.dispatch_to['method'] and \
                        method_name in self.exposed_methods:
                    method.assert_called_once_with(
                        self.ctxt, **self.msg.get('args', {}))
                else:
                    self.assertEqual(0, method.call_count,
                                     f'method: {method}')


class TestDispatcherWithPingEndpoint(test_utils.BaseTestCase):
    def test_dispatcher_with_ping(self):
        self.config(rpc_ping_enabled=True)
        dispatcher = oslo_messaging.RPCDispatcher([], None, None)
        incoming = mock.Mock(ctxt={},
                             message=dict(method='oslo_rpc_server_ping'),
                             client_timeout=0)

        res = dispatcher.dispatch(incoming)
        self.assertEqual('pong', res)

    def test_dispatcher_with_ping_already_used(self):
        class MockEndpoint:
            def oslo_rpc_server_ping(self, ctxt, **kwargs):
                return 'not_pong'

        mockEndpoint = MockEndpoint()

        self.config(rpc_ping_enabled=True)
        dispatcher = oslo_messaging.RPCDispatcher([mockEndpoint], None, None)
        incoming = mock.Mock(ctxt={},
                             message=dict(method='oslo_rpc_server_ping'),
                             client_timeout=0)

        res = dispatcher.dispatch(incoming)
        self.assertEqual('not_pong', res)


class TestSerializer(test_utils.BaseTestCase):
    scenarios = [
        ('no_args_or_retval',
         dict(ctxt={}, dctxt={}, args={}, retval=None)),
        ('args_and_retval',
         dict(ctxt=dict(user='bob'),
              dctxt=dict(user='alice'),
              args=dict(a='a', b='b', c='c'),
              retval='d')),
    ]

    def test_serializer(self):
        endpoint = _FakeEndpoint()
        serializer = msg_serializer.NoOpSerializer()
        dispatcher = oslo_messaging.RPCDispatcher([endpoint], serializer)

        endpoint.foo = mock.Mock()

        args = {k: 'd' + v for k, v in self.args.items()}
        endpoint.foo.return_value = self.retval

        serializer.serialize_entity = mock.Mock()
        serializer.deserialize_entity = mock.Mock()
        serializer.deserialize_context = mock.Mock()

        serializer.deserialize_context.return_value = self.dctxt

        expected_side_effect = ['d' + arg for arg in self.args]
        serializer.deserialize_entity.side_effect = expected_side_effect

        serializer.serialize_entity.return_value = None
        if self.retval:
            serializer.serialize_entity.return_value = 's' + self.retval

        incoming = mock.Mock()
        incoming.ctxt = self.ctxt
        incoming.message = dict(method='foo', args=self.args)
        incoming.client_timeout = 0
        retval = dispatcher.dispatch(incoming)
        if self.retval is not None:
            self.assertEqual('s' + self.retval, retval)

        endpoint.foo.assert_called_once_with(self.dctxt, **args)
        serializer.deserialize_context.assert_called_once_with(self.ctxt)

        expected_calls = [mock.call(self.dctxt, arg) for arg in self.args]
        self.assertEqual(expected_calls,
                         serializer.deserialize_entity.mock_calls)

        serializer.serialize_entity.assert_called_once_with(self.dctxt,
                                                            self.retval)


class TestMonitorFailure(test_utils.BaseTestCase):
    """Test what happens when the call monitor watchdog hits an exception when
    sending the heartbeat.
    """

    class _SleepyEndpoint:
        def __init__(self, target=None):
            self.target = target

        def sleep(self, ctxt, **kwargs):
            time.sleep(kwargs['timeout'])
            return True

    def test_heartbeat_failure(self):

        endpoints = [self._SleepyEndpoint()]
        dispatcher = oslo_messaging.RPCDispatcher(endpoints,
                                                  serializer=None)

        # sleep long enough for the client_timeout to expire multiple times
        # the timeout is (client_timeout/2) and must be > 1.0
        message = {'method': 'sleep',
                   'args': {'timeout': 3.5}}
        ctxt = {'test': 'value'}

        incoming = mock.Mock(ctxt=ctxt, message=message, client_timeout=2.0)
        incoming.heartbeat = mock.Mock(side_effect=Exception('BOOM!'))
        res = dispatcher.dispatch(incoming)
        self.assertTrue(res)

        # only one call to heartbeat should be made since the watchdog thread
        # should exit on the first exception thrown
        self.assertEqual(1, incoming.heartbeat.call_count)


class TestWatchdogLeakOnFailure(test_utils.BaseTestCase):
    """Regression tests for a call-monitor watchdog thread leak.

    RPCDispatcher.dispatch() starts the watchdog thread before iterating
    the endpoint list, but signals completion_event / joins the watchdog
    only inside the successful-dispatch try/finally.  Three failure paths
    raise without stopping the watchdog:

      1. NoSuchMethod        - endpoint matches namespace/version but
                               lacks the requested method.
      2. Access policy deny  - same fall-through as NoSuchMethod when
                               access_policy.is_allowed() returns False.
      3. UnsupportedVersion  - no endpoint matches namespace/version.

    A leaked watchdog keeps calling incoming.heartbeat() every
    client_timeout/2 seconds for a request the server has already
    rejected.
    """

    # Watchdog fires every client_timeout/2 == 1 s once client_timeout=2.
    # Wait slightly more than that so a leaked watchdog fires at least once.
    WATCHDOG_QUIESCE_S = 1.2

    def _make_incoming(self, method, namespace=None):
        incoming = mock.Mock()
        incoming.message = {'method': method, 'args': {},
                            'namespace': namespace, 'version': '1.0'}
        incoming.ctxt = {}
        # client_timeout must be int >= 2 for the watchdog to fire
        # (cm_heartbeat_interval = int(client_timeout) / 2 >= 1).
        incoming.client_timeout = 2
        return incoming

    def _assert_no_heartbeat_after_raise(self, incoming):
        time.sleep(self.WATCHDOG_QUIESCE_S)
        self.assertEqual(
            0, incoming.heartbeat.call_count,
            'Watchdog leaked: incoming.heartbeat() was called '
            f'{incoming.heartbeat.call_count} time(s) after dispatch() '
            'raised. The watchdog thread was started but never signalled '
            'via completion_event.set() on the failure path.')

    def test_no_such_method_joins_watchdog(self):
        """NoSuchMethod: endpoint matches namespace/version, lacks method."""
        class _Endpoint:
            target = oslo_messaging.Target()

            def ping(self, ctxt):  # pragma: no cover
                return 'pong'

        dispatcher = oslo_messaging.RPCDispatcher(
            [_Endpoint()], serializer=None,
            access_policy=oslo_messaging.LegacyRPCAccessPolicy)
        incoming = self._make_incoming('unknown_method')

        self.assertRaises(oslo_messaging.NoSuchMethod,
                          dispatcher.dispatch, incoming)
        self._assert_no_heartbeat_after_raise(incoming)

    def test_access_denied_joins_watchdog(self):
        """Access policy denies the call; falls through as NoSuchMethod."""
        class _Endpoint:
            target = oslo_messaging.Target()

            def _private(self, ctxt):  # pragma: no cover
                return 'private'

        dispatcher = oslo_messaging.RPCDispatcher(
            [_Endpoint()], serializer=None,
            access_policy=oslo_messaging.ExplicitRPCAccessPolicy)
        incoming = self._make_incoming('_private')

        self.assertRaises(oslo_messaging.NoSuchMethod,
                          dispatcher.dispatch, incoming)
        self._assert_no_heartbeat_after_raise(incoming)

    def test_unsupported_version_joins_watchdog(self):
        """UnsupportedVersion: no endpoint's namespace matches the request."""
        class _Endpoint:
            target = oslo_messaging.Target(namespace='other')

            def ping(self, ctxt):  # pragma: no cover
                return 'pong'

        dispatcher = oslo_messaging.RPCDispatcher(
            [_Endpoint()], serializer=None,
            access_policy=oslo_messaging.LegacyRPCAccessPolicy)
        incoming = self._make_incoming('ping', namespace=None)

        self.assertRaises(oslo_messaging.UnsupportedVersion,
                          dispatcher.dispatch, incoming)
        self._assert_no_heartbeat_after_raise(incoming)

    def test_no_endpoints_at_all_joins_watchdog(self):
        """UnsupportedVersion on an empty endpoint list also cleans up."""
        dispatcher = oslo_messaging.RPCDispatcher(
            [], serializer=None,
            access_policy=oslo_messaging.LegacyRPCAccessPolicy)
        incoming = self._make_incoming('ping')

        self.assertRaises(oslo_messaging.UnsupportedVersion,
                          dispatcher.dispatch, incoming)
        self._assert_no_heartbeat_after_raise(incoming)
