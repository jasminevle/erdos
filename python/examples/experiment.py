"""Merges messages from two senders based on message number (count).
A MeasurementOp finds the difference between the max system time and the min system time
of the two timestamps in a joined message.
If logical time is used for timestamps, the difference is in seconds.
If system time is used for timestamps, the difference is in microseconds.
"""

import time

from erdos import utils
import erdos


class SendOp(erdos.Operator):
    """Sends `frequency` messages per second."""
    def __init__(self, write_stream, frequency, duration):
        self.frequency = frequency
        self.write_stream = write_stream
        self.duration = duration

    @staticmethod
    def connect():
        return [erdos.WriteStream()]

    def run(self):
        count = 0
        max_msgs = self.duration*self.frequency
        while count < max_msgs:
            timestamp = erdos.Timestamp(coordinates=[round(count / frequency * 1000)])
            msg = erdos.Message(timestamp, (time.time(), count))

            print("{name}: sending {msg}".format(name=self.config.name, msg=msg))
            self.write_stream.send(msg)

            watermark = erdos.WatermarkMessage(timestamp)
            print("{name}: sending watermark {watermark}".format(
                name=self.config.name, watermark=watermark))
            self.write_stream.send(watermark)

            count += 1
            time.sleep(1 / self.frequency)


class MostPermissiveJoinOp(erdos.Operator):
    def __init__(self, left_stream, right_stream, write_stream, log_file):
        self.left_msgs = []
        self.right_msgs = []

        self.left_reccnt, self.left_usedcnt, self.left_duplcnt = 0, 0, 0
        self.right_reccnt, self.right_usedcnt, self.right_duplcnt = 0, 0, 0
        self.joincnt = 0
        self.logger = utils.setup_csv_logging("most permissive completeness and cardinality",
                                              log_file=log_file)

        erdos.add_callback([left_stream], [write_stream], self.recv_left)
        erdos.add_callback([right_stream], [write_stream], self.recv_right)

    # TODO: use a callback on a stateful read stream instead of passing self
    def recv_left(self, msg, write_stream):
        print("MostPermissiveJoinOp: received {msg} on left stream".format(msg=msg))
        self.left_msgs.append(msg)
        self.left_reccnt += 1
        self.send_joined_left(msg, write_stream)

    def recv_right(self, msg, write_stream):
        print("MostPermissiveJoinOp: received {msg} on right stream".format(msg=msg))
        self.right_msgs.append(msg)
        self.right_reccnt += 1
        self.send_joined_right(msg, write_stream)

    def count_and_log(self):
        self.joincnt += 1
        self.logger.warning(
            "{left_total}, {left_used}, {left_duplicated}, {right_total}, {right_used}, {right_duplicated}, {cardinality}".format(
                left_total=self.left_reccnt,
                left_used=self.left_usedcnt,
                left_duplicated=self.left_duplcnt,
                right_total=self.right_reccnt,
                right_used=self.right_usedcnt,
                right_duplicated=self.right_duplcnt,
                cardinality=self.joincnt
                ))

    def send_joined_left(self, msg, write_stream):
        self.left_usedcnt += 1
        for right_msg in self.right_msgs:
            self.right_duplcnt += 1
            timestamp = msg.timestamp
            joined_msg = erdos.Message(timestamp, (msg.data[0], right_msg.data[0]))
            self.count_and_log()
            print("MostPermissiveJoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
            write_stream.send(joined_msg)

    def send_joined_right(self, msg, write_stream):
        self.right_usedcnt += 1
        for left_msg in self.left_msgs:
            self.left_duplcnt += 1
            timestamp = msg.timestamp
            joined_msg = erdos.Message(timestamp, (msg.data[0], left_msg.data[0]))
            self.count_and_log()
            print("MostPermissiveJoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
            write_stream.send(joined_msg)

    @staticmethod
    def connect(left_stream, right_stream):
        return [erdos.WriteStream()]


class TimestampJoinOp(erdos.Operator):
    def __init__(self, left_stream, right_stream, write_stream, log_file):
        self.left_msgs = {}
        self.right_msgs = {}

        self.left_reccnt, self.left_usedcnt, self.left_duplcnt = 0, 0, 0
        self.right_reccnt, self.right_usedcnt, self.right_duplcnt = 0, 0, 0
        self.joincnt = 0
        self.logger = utils.setup_csv_logging("timestamp completeness and cardinality",
                                              log_file=log_file)
        left_stream.add_callback(self.recv_left)
        right_stream.add_callback(self.recv_right)
        erdos.add_watermark_callback([left_stream, right_stream],
                                     [write_stream], self.send_joined)

    # TODO: use a callback on a stateful read stream instead of passing self
    def recv_left(self, msg):
        print("TimestampJoinOp: received {msg} on left stream".format(msg=msg))
        self.left_msgs[msg.timestamp] = msg

    def recv_right(self, msg):
        print("TimestampJoinOp: received {msg} on right stream".format(msg=msg))
        self.right_msgs[msg.timestamp] = msg

    def count_and_log(self):
        self.joincnt += 1
        self.logger.warning(
            "{left_total}, {left_used}, {left_duplicated}, {right_total}, {right_used}, {right_duplicated}, {cardinality}".format(
                left_total=self.left_reccnt,
                left_used=self.left_usedcnt,
                left_duplicated=self.left_duplcnt,
                right_total=self.right_reccnt,
                right_used=self.right_usedcnt,
                right_duplicated=self.right_duplcnt,
                cardinality=self.joincnt
                ))

    def send_joined(self, timestamp, write_stream):
        left_msg = self.left_msgs.pop(timestamp)
        right_msg = self.right_msgs.pop(timestamp)
        joined_msg = erdos.Message(timestamp, (left_msg.data[0], right_msg.data[0]))
        print("TimestampJoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
        write_stream.send(joined_msg)

    @staticmethod
    def connect(left_stream, right_stream):
        return [erdos.WriteStream()]


class RecentNoDuplJoinOp(erdos.Operator):
    def __init__(self, left_stream, right_stream, write_stream, log_file):
        self.left_msgs = []
        self.right_msgs = []

        self.left_reccnt, self.left_usedcnt, self.left_duplcnt = 0, 0, 0
        self.right_reccnt, self.right_usedcnt, self.right_duplcnt = 0, 0, 0
        self.joincnt = 0
        self.logger = utils.setup_csv_logging("recent on dupl completeness and cardinality",
                                              log_file=log_file)

        erdos.add_callback([left_stream], [write_stream], self.recv_left)
        erdos.add_callback([right_stream], [write_stream], self.recv_right)

    # TODO: use a callback on a stateful read stream instead of passing self
    def recv_left(self, msg, write_stream):
        print("RecentNoDuplJoinOp: received {msg} on left stream".format(msg=msg))
        self.left_msgs[0] = msg
        self.left_reccnt += 1
        self.send_joined(write_stream)

    def recv_right(self, msg, write_stream):
        print("RecentNoDuplJoinOp: received {msg} on right stream".format(msg=msg))
        self.right_msgs[0] = msg
        self.right_reccnt += 1
        self.send_joined(write_stream)

    def count_and_log(self):
        self.joincnt += 1
        self.logger.warning(
            "{left_total}, {left_used}, {left_duplicated}, {right_total}, {right_used}, {right_duplicated}, {cardinality}".format(
                left_total=self.left_reccnt,
                left_used=self.left_usedcnt,
                left_duplicated=self.left_duplcnt,
                right_total=self.right_reccnt,
                right_used=self.right_usedcnt,
                right_duplicated=self.right_duplcnt,
                cardinality=self.joincnt
                ))

    def send_joined(self, write_stream):
        if len(self.left_msgs) == 0 or len(self.right_msgs) == 0:
            return
        left_msg, right_msg = self.left_msgs.pop(0), self.right_msgs.pop(0)
        self.left_used += 1
        self.right_used += 1
        timestamp = max(left_msg.timestamp, right_msg.timestamp)
        joined_msg = erdos.Message(timestamp, (left_msg.data[0], right_msg.data[0]))
        self.count_and_log()
        print("RecentNoDuplJoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
        write_stream.send(joined_msg)

    @staticmethod
    def connect(left_stream, right_stream):
        return [erdos.WriteStream()]

##TO DO: implement PermissiveRecentJoinOp


class MeasurementOp(erdos.Operator):
    def __init__(self, read_stream, write_stream):
        read_stream.add_callback(self.callback, [write_stream])
        self.logger = utils.setup_csv_logging("time data",
                                              log_file="time data")

    def callback(self, msg, write_stream):
        current_time = time.time()

        print("MeasurementOp: receiving {msg}".format(msg=msg))
        left_data, right_data = msg.data[0], msg.data[1]
        left_time, right_time = left_data[0], right_data[0]
        
        timestamp = msg.timestamp
        difference = abs(left_time-right_time)
        left_recency = abs(left_time-current_time)
        right_recency = abs(right_time-current_time)
        data = (timestamp, difference, left_recency, right_recency)

        msg = erdos.Message(msg.timestamp, data)
        self.logger.warning(
            "{timestamp}, {time_difference}, {left_recency}, {right_recency}".format(
                timestamp = msg.timestamp,
                time_difference=difference,
                left_recency=left_recency,
                right_recency=right_recency
                ))

        print("MeasurementOp: sending {msg}".format(msg=msg))
        write_stream.send(msg)

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]


def main():
    """Creates and runs the dataflow graph."""
    f_1, f_2, d_1, d_2 = 1, 2, 100, 100
    #1 = permissive recent, 2 = recent with no duplicates, 3 = timestamp
    jointype = 3
    log_file = "dsjhdka"

    (left_stream, ) = erdos.connect(SendOp,
                                    erdos.OperatorConfig(name="F_1SendOp"), [],
                                    frequency=f_1, duration=d_1)
    (right_stream, ) = erdos.connect(SendOp,
                                     erdos.OperatorConfig(name="F_2SendOp"),
                                     [],
                                     frequency=f_2, duration=d_2)
    (join_stream, ) = erdos.connect(JoinOp,
                                    erdos.OperatorConfig(),
                                    [left_stream, right_stream], jointype=jointype, log_file=log_file)
    (time_stream, ) = erdos.connect(MeasurementOp,
                                    erdos.OperatorConfig(), [join_stream])
    erdos.run()


if __name__ == "__main__":
    main()
