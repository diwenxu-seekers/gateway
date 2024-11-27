from datetime import datetime, time
import pytz

class OrderSubSenderID:
    """
    This class is related to the implementation of FIX tag 50.

    Given a configuration of periods and their corresponding each sender IDs,
    the instance will tell which sender ID should be used.

    Sample configuration:
     - (HH:MM, HH:MM, timezone, sender_id)

    """
    def __init__(self, shifts: list) -> None:
        self._handlers = [self._create_handler(*s) for s in shifts]

    @staticmethod
    def create(filepath):
        shifts = []
        with open(filepath, 'r') as fd:
            line = fd.readline()
            while line:
                tokens = [x.strip() for x in line.split(',')]
                shifts.append(tokens)
                line = fd.readline()

        return OrderSubSenderID(shifts)

    def active(self, time=None):
        """
        Returns the sender id that is active.
        The first record that matches takes precedence.
        """
        _time = pytz.utc.localize(datetime.utcnow()) if time is None else time
        match = [hdlr(_time) for hdlr in self._handlers]
        senders = list(x for x in match if x)
        if senders:
            return senders[0]
        return ''

    def _create_handler(self, begin, end, timezone, sender_id):
        tz = pytz.timezone(timezone)
        t1 = self._parse_time(begin, tz)
        t2 = self._parse_time(end, tz)

        def test(time_utc):
            t = time_utc.astimezone(tz).time()
            if t1 == t2:
                raise Exception("Start time and end time should not be the same.")
            elif t1 < t2:
                if t1 < t <= t2:
                    return sender_id
            else:
                if t > t1 or t <= t2:
                    return sender_id
            return ''
        return test

    def _parse_time(self, s: str, tz):
        tokens = s.strip().split(':')
        return time(hour=int(tokens[0]), minute=int(tokens[1]), tzinfo=tz)


if __name__ == "__main__":
    sender_id = OrderSubSenderID([
        ("05:35", "04:50", "America/Chicago", "Luke"),
        ("18:00", "20:00", "America/Chicago", "David"),
        ("17:32", "20:00", "Asia/Hong_Kong", "John"),
    ])
    print(sender_id.active())