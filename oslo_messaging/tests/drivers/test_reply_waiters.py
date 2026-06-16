# Copyright 2024 SAP SE
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

"""Unit tests for ReplyWaiters.get — blocking and polling paths."""

import queue
import threading
import time
import unittest
from unittest.mock import patch

import oslo_messaging
from oslo_messaging._drivers.amqpdriver import ReplyWaiters


class TestReplyWaitersBlocking(unittest.TestCase):
    """_use_blocking_get=True path: single queue.get(block=True, timeout=…)."""

    def _make(self):
        """Return a ReplyWaiters wired for the blocking path."""
        # _use_blocking_get = not (_is_eventlet and heartbeat_enabled)
        # Force the blocking path by patching _is_eventlet to False.
        with patch('oslo_messaging._drivers.amqpdriver._is_eventlet', False):
            rw = ReplyWaiters(heartbeat_enabled=True)
        self.assertTrue(rw._use_blocking_get)
        return rw

    def test_returns_reply_when_available_immediately(self):
        rw = self._make()
        msg_id = 'msg-1'
        rw.add(msg_id)
        rw.put(msg_id, {'result': 42})
        result = rw.get(msg_id, timeout=5.0)
        self.assertEqual({'result': 42}, result)

    def test_returns_reply_delivered_after_short_delay(self):
        rw = self._make()
        msg_id = 'msg-delay'
        rw.add(msg_id)

        def _deliver():
            time.sleep(0.05)
            rw.put(msg_id, {'result': 'ok'})

        t = threading.Thread(target=_deliver)
        t.start()
        result = rw.get(msg_id, timeout=2.0)
        t.join()
        self.assertEqual({'result': 'ok'}, result)

    def test_raises_messaging_timeout_when_queue_empty(self):
        rw = self._make()
        msg_id = 'msg-timeout'
        rw.add(msg_id)
        self.assertRaises(
            oslo_messaging.MessagingTimeout,
            rw.get, msg_id, 0.05,
        )

    def test_no_stopwatch_used_on_blocking_path(self):
        """StopWatch must not be instantiated on the blocking path."""
        rw = self._make()
        msg_id = 'msg-nowatch'
        rw.add(msg_id)
        rw.put(msg_id, 'data')

        with patch('oslo_messaging._drivers.amqpdriver.timeutils') as mock_tu:
            # timeutils.StopWatch should never be called on this path
            result = rw.get(msg_id, timeout=1.0)

        mock_tu.StopWatch.assert_not_called()
        self.assertEqual('data', result)


class TestReplyWaitersPolling(unittest.TestCase):
    """_use_blocking_get=False path: StopWatch + polling with sleep(0.005)."""

    def _make(self):
        """Return a ReplyWaiters wired for the polling path (eventlet+hb)."""
        with patch('oslo_messaging._drivers.amqpdriver._is_eventlet', True):
            rw = ReplyWaiters(heartbeat_enabled=True)
        self.assertFalse(rw._use_blocking_get)
        return rw

    def test_returns_reply_when_available_immediately(self):
        rw = self._make()
        msg_id = 'poll-1'
        rw.add(msg_id)
        rw.put(msg_id, {'result': 7})
        result = rw.get(msg_id, timeout=2.0)
        self.assertEqual({'result': 7}, result)

    def test_returns_reply_delivered_after_short_delay(self):
        rw = self._make()
        msg_id = 'poll-delay'
        rw.add(msg_id)

        def _deliver():
            time.sleep(0.02)
            rw.put(msg_id, 'late')

        t = threading.Thread(target=_deliver)
        t.start()
        result = rw.get(msg_id, timeout=2.0)
        t.join()
        self.assertEqual('late', result)

    def test_raises_messaging_timeout_when_queue_stays_empty(self):
        rw = self._make()
        msg_id = 'poll-timeout'
        rw.add(msg_id)
        self.assertRaises(
            oslo_messaging.MessagingTimeout,
            rw.get, msg_id, 0.02,
        )

    def test_sleep_interval_is_005(self):
        """Polling loop must sleep 0.005 s between attempts, not 0.5."""
        rw = self._make()
        msg_id = 'poll-sleep'
        rw.add(msg_id)

        sleep_calls = []

        original_sleep = time.sleep

        def _recording_sleep(secs):
            sleep_calls.append(secs)
            original_sleep(secs)

        with patch('oslo_messaging._drivers.amqpdriver.time.sleep',
                   side_effect=_recording_sleep):
            self.assertRaises(
                oslo_messaging.MessagingTimeout,
                rw.get, msg_id, 0.02,
            )

        self.assertTrue(len(sleep_calls) >= 1,
                        "Expected at least one sleep call")
        for s in sleep_calls:
            self.assertAlmostEqual(0.005, s,
                                   msg=f"sleep({s}) != 0.005")

    def test_stopwatch_used_on_polling_path(self):
        """StopWatch must be used to bound the polling loop."""
        rw = self._make()
        msg_id = 'poll-watch'
        rw.add(msg_id)
        rw.put(msg_id, 'value')

        with patch('oslo_messaging._drivers.amqpdriver.timeutils.StopWatch',
                   wraps=__import__(
                       'oslo_utils.timeutils', fromlist=['StopWatch']
                   ).StopWatch) as mock_sw:
            result = rw.get(msg_id, timeout=1.0)

        mock_sw.assert_called_once_with(duration=1.0)
        self.assertEqual('value', result)


if __name__ == '__main__':
    unittest.main()
