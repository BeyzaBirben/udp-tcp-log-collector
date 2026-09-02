import asyncio
import math
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from log_collector.email_delivery import (
    AsyncEmailDispatcher,
    EmailNotification,
    SMTPEmailSender,
    SMTPSettings,
)


def notification(
    alarm_id: str = "alarm-1",
    rule_name: str = "SSH Login Failed",
) -> EmailNotification:
    return EmailNotification(
        alarm_id=alarm_id,
        rule_id="45302f50-35e0-4da8-b767-aaa647004637",
        rule_name=rule_name,
        source_ip="192.168.1.15",
        log_id="8c831afa-2695-4cec-a19c-06caa73180a7",
        raw_log="sshd: login failed",
        recipients=("soc@example.com", "ops@example.com"),
    )


class SMTPSettingsTests(unittest.TestCase):
    def test_loads_mailhog_defaults(self) -> None:
        settings = SMTPSettings.from_environment({})

        self.assertEqual(settings.host, "mailhog")
        self.assertEqual(settings.port, 1025)
        self.assertEqual(settings.from_address, "alerts@log-collector.local")
        self.assertIsNone(settings.username)
        self.assertIsNone(settings.password)
        self.assertFalse(settings.use_tls)
        self.assertEqual(settings.timeout_seconds, 5.0)

    def test_loads_authenticated_tls_configuration(self) -> None:
        settings = SMTPSettings.from_environment(
            {
                "SMTP_HOST": " smtp.example.com ",
                "SMTP_PORT": "587",
                "SMTP_FROM": " alerts@example.com ",
                "SMTP_USERNAME": " mail-user ",
                "SMTP_PASSWORD": " secret ",
                "SMTP_USE_TLS": "true",
                "SMTP_TIMEOUT_SECONDS": "2.5",
            }
        )

        self.assertEqual(settings.host, "smtp.example.com")
        self.assertEqual(settings.port, 587)
        self.assertEqual(settings.from_address, "alerts@example.com")
        self.assertEqual(settings.username, "mail-user")
        self.assertEqual(settings.password, "secret")
        self.assertTrue(settings.use_tls)
        self.assertEqual(settings.timeout_seconds, 2.5)

    def test_rejects_invalid_configuration(self) -> None:
        invalid_values = (
            ({"SMTP_HOST": "  "}, "SMTP_HOST"),
            ({"SMTP_PORT": "0"}, "SMTP_PORT"),
            ({"SMTP_PORT": "invalid"}, "SMTP_PORT"),
            ({"SMTP_FROM": "invalid"}, "SMTP_FROM"),
            ({"SMTP_USERNAME": "user"}, "configured together"),
            ({"SMTP_PASSWORD": "secret"}, "configured together"),
            ({"SMTP_USE_TLS": "invalid"}, "SMTP_USE_TLS"),
            ({"SMTP_TIMEOUT_SECONDS": "0"}, "SMTP_TIMEOUT_SECONDS"),
            ({"SMTP_TIMEOUT_SECONDS": "nan"}, "SMTP_TIMEOUT_SECONDS"),
        )

        for values, message in invalid_values:
            with self.subTest(values=values):
                with self.assertRaisesRegex(ValueError, message):
                    SMTPSettings.from_environment(values)

        with self.assertRaisesRegex(ValueError, "SMTP_USE_TLS"):
            SMTPSettings(use_tls=1)

        with self.assertRaisesRegex(ValueError, "SMTP_TIMEOUT_SECONDS"):
            SMTPSettings(timeout_seconds=math.inf)

        with self.assertRaisesRegex(ValueError, "SMTP_TIMEOUT_SECONDS"):
            SMTPSettings(timeout_seconds=True)

        with self.assertRaisesRegex(ValueError, "SMTP_USERNAME"):
            SMTPSettings(username=" ", password="secret")

    def test_notification_requires_a_recipient(self) -> None:
        with self.assertRaisesRegex(ValueError, "recipient"):
            EmailNotification(
                alarm_id="alarm-1",
                rule_id="rule-1",
                rule_name="Rule",
                source_ip="192.168.1.15",
                log_id="log-1",
                raw_log="failed",
                recipients=(),
            )


class SMTPEmailSenderTests(unittest.IsolatedAsyncioTestCase):
    async def test_sends_a_structured_message_without_authentication(self) -> None:
        smtp_client = MagicMock()
        smtp_context = MagicMock()
        smtp_context.__enter__.return_value = smtp_client
        sender = SMTPEmailSender(SMTPSettings())

        with patch(
            "log_collector.email_delivery.smtplib.SMTP",
            return_value=smtp_context,
        ) as smtp_factory:
            await sender.send(notification())

        smtp_factory.assert_called_once_with("mailhog", 1025, timeout=5.0)
        smtp_client.ehlo.assert_called_once()
        smtp_client.starttls.assert_not_called()
        smtp_client.login.assert_not_called()
        smtp_client.send_message.assert_called_once()
        message = smtp_client.send_message.call_args.args[0]
        self.assertEqual(message["From"], "alerts@log-collector.local")
        self.assertEqual(message["To"], "soc@example.com, ops@example.com")
        self.assertEqual(message["Subject"], "[Log Collector] SSH Login Failed")
        body = message.get_content()
        self.assertIn("Alarm ID: alarm-1", body)
        self.assertIn("Source IP: 192.168.1.15", body)
        self.assertIn("sshd: login failed", body)

    async def test_supports_starttls_and_authentication(self) -> None:
        smtp_client = MagicMock()
        smtp_context = MagicMock()
        smtp_context.__enter__.return_value = smtp_client
        tls_context = object()
        sender = SMTPEmailSender(
            SMTPSettings(
                host="smtp.example.com",
                port=587,
                from_address="alerts@example.com",
                username="mail-user",
                password="secret",
                use_tls=True,
            )
        )

        with (
            patch(
                "log_collector.email_delivery.smtplib.SMTP",
                return_value=smtp_context,
            ),
            patch(
                "log_collector.email_delivery.ssl.create_default_context",
                return_value=tls_context,
            ),
        ):
            await sender.send(notification())

        self.assertEqual(smtp_client.ehlo.call_count, 2)
        smtp_client.starttls.assert_called_once_with(context=tls_context)
        smtp_client.login.assert_called_once_with("mail-user", "secret")
        smtp_client.send_message.assert_called_once()

    async def test_normalizes_rule_name_in_message_subject(self) -> None:
        smtp_client = MagicMock()
        smtp_context = MagicMock()
        smtp_context.__enter__.return_value = smtp_client
        sender = SMTPEmailSender(SMTPSettings())

        with patch(
            "log_collector.email_delivery.smtplib.SMTP",
            return_value=smtp_context,
        ):
            await sender.send(
                notification(rule_name="SSH Login\r\nFailed")
            )

        message = smtp_client.send_message.call_args.args[0]
        self.assertEqual(message["Subject"], "[Log Collector] SSH Login Failed")


class AsyncEmailDispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.sender = MagicMock()
        self.sender.send = AsyncMock()
        self.status_writer = MagicMock()
        self.status_writer.mark_email_sent = AsyncMock(return_value=True)
        self.dispatcher = AsyncEmailDispatcher(
            self.sender,
            self.status_writer,
        )

    async def asyncTearDown(self) -> None:
        await self.dispatcher.close()

    async def test_marks_alarm_sent_only_after_successful_delivery(self) -> None:
        item = notification()

        await self.dispatcher.enqueue(item)
        await self.dispatcher.close()

        self.sender.send.assert_awaited_once_with(item)
        self.status_writer.mark_email_sent.assert_awaited_once_with("alarm-1")

    async def test_smtp_failure_leaves_email_status_false(self) -> None:
        self.sender.send.side_effect = RuntimeError("SMTP unavailable")

        with self.assertLogs("log_collector.email_delivery", level="ERROR"):
            await self.dispatcher.enqueue(notification())
            await self.dispatcher.close()

        self.status_writer.mark_email_sent.assert_not_awaited()

    async def test_status_update_failure_does_not_crash_worker(self) -> None:
        self.status_writer.mark_email_sent.side_effect = RuntimeError(
            "database unavailable"
        )

        with self.assertLogs("log_collector.email_delivery", level="ERROR"):
            await self.dispatcher.enqueue(notification())
            await self.dispatcher.close()

        self.sender.send.assert_awaited_once()

    async def test_missing_alarm_is_logged_without_stopping_worker(self) -> None:
        self.status_writer.mark_email_sent.return_value = False

        with self.assertLogs("log_collector.email_delivery", level="WARNING"):
            await self.dispatcher.enqueue(notification())
            await self.dispatcher.close()

        self.sender.send.assert_awaited_once()

    async def test_failure_of_one_email_does_not_stop_later_emails(self) -> None:
        self.sender.send.side_effect = [RuntimeError("SMTP error"), None]

        with self.assertLogs("log_collector.email_delivery", level="ERROR"):
            await self.dispatcher.enqueue(notification("alarm-1"))
            await self.dispatcher.enqueue(notification("alarm-2"))
            await self.dispatcher.close()

        self.assertEqual(self.sender.send.await_count, 2)
        self.status_writer.mark_email_sent.assert_awaited_once_with("alarm-2")

    async def test_enqueue_returns_while_smtp_delivery_is_still_running(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        async def wait_for_release(item: EmailNotification) -> None:
            started.set()
            await release.wait()

        self.sender.send.side_effect = wait_for_release

        await self.dispatcher.enqueue(notification())
        await asyncio.wait_for(started.wait(), timeout=1.0)

        self.status_writer.mark_email_sent.assert_not_awaited()

        release.set()
        await self.dispatcher.close()

        self.status_writer.mark_email_sent.assert_awaited_once()

    async def test_start_and_close_are_idempotent(self) -> None:
        await asyncio.gather(
            self.dispatcher.start(),
            self.dispatcher.start(),
        )
        await self.dispatcher.close()
        await self.dispatcher.close()

        self.sender.send.assert_not_awaited()
