from multiprocessing import Process
import time
import os
import signal

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
            timestamp = erdos.Timestamp(coordinates=[round(count / self.frequency * 1000)])
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
        self.logger.warning("left_total, left_used, left_duplicated, right_total, right_used, right_duplicated, cardinality")
        left_stream.add_callback(self.recv_left, [write_stream])
        right_stream.add_callback(self.recv_right, [write_stream])

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
            joined_msg = erdos.Message(timestamp, (msg.data[0], msg.timestamp, right_msg.data[0], right_msg.timestamp))
            self.count_and_log()
            print("MostPermissiveJoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
            write_stream.send(joined_msg)

    def send_joined_right(self, msg, write_stream):
        self.right_usedcnt += 1
        for left_msg in self.left_msgs:
            self.left_duplcnt += 1
            timestamp = msg.timestamp
            joined_msg = erdos.Message(timestamp, (left_msg.data[0], left_msg.timestamp, msg.data[0], msg.timestamp))
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
        self.logger.warning("left_total, left_used, left_duplicated, right_total, right_used, right_duplicated, cardinality")
        left_stream.add_callback(self.recv_left)
        right_stream.add_callback(self.recv_right)
        erdos.add_watermark_callback([left_stream, right_stream],
                                     [write_stream], self.send_joined)

    # TODO: use a callback on a stateful read stream instead of passing self
    def recv_left(self, msg):
        print("TimestampJoinOp: received {msg} on left stream".format(msg=msg))
        self.left_msgs[msg.timestamp] = msg
        self.left_reccnt += 1

    def recv_right(self, msg):
        print("TimestampJoinOp: received {msg} on right stream".format(msg=msg))
        self.right_msgs[msg.timestamp] = msg
        self.right_reccnt += 1

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
        if self.left_msgs.get(timestamp) is None or self.right_msgs.get(timestamp) is None:
            return
        left_msg = self.left_msgs.pop(timestamp)
        right_msg = self.right_msgs.pop(timestamp)
        self.left_usedcnt += 1
        self.right_usedcnt += 1
        joined_msg = erdos.Message(timestamp, (left_msg.data[0], left_msg.timestamp, right_msg.data[0], right_msg.timestamp))
        self.count_and_log()
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
        self.logger.warning("left_total, left_used, left_duplicated, right_total, right_used, right_duplicated, cardinality")

        left_stream.add_callback(self.recv_left, [write_stream])
        right_stream.add_callback(self.recv_right, [write_stream])

    # TODO: use a callback on a stateful read stream instead of passing self
    def recv_left(self, msg, write_stream):
        print("RecentNoDuplJoinOp: received {msg} on left stream".format(msg=msg))
        if len(self.left_msgs) == 0:
            self.left_msgs.append(msg)
        self.left_msgs[0] = msg
        self.left_reccnt += 1
        self.send_joined(write_stream)

    def recv_right(self, msg, write_stream):
        print("RecentNoDuplJoinOp: received {msg} on right stream".format(msg=msg))
        if len(self.right_msgs) == 0:
            self.right_msgs.append(msg)
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
        self.left_usedcnt += 1
        self.right_usedcnt += 1
        timestamp = max(left_msg.timestamp, right_msg.timestamp)
        joined_msg = erdos.Message(timestamp, (left_msg.data[0], left_msg.timestamp, right_msg.data[0], right_msg.timestamp))
        self.count_and_log()
        print("RecentNoDuplJoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
        write_stream.send(joined_msg)

    @staticmethod
    def connect(left_stream, right_stream):
        return [erdos.WriteStream()]


class PermissiveRecentJoinOp(erdos.Operator):
    def __init__(self, left_stream, right_stream, write_stream, log_file):
        self.left_msgs = []
        self.right_msgs = []

        self.left_reccnt, self.left_usedcnt, self.left_duplcnt = 0, 0, 0
        self.right_reccnt, self.right_usedcnt, self.right_duplcnt = 0, 0, 0
        self.joincnt = 0
        self.logger = utils.setup_csv_logging("permissive recent completeness and cardinality",
                                              log_file=log_file)
        self.logger.warning("left_total, left_used, left_duplicated, right_total, right_used, right_duplicated, cardinality")

        left_stream.add_callback(self.recv_left, [write_stream])
        right_stream.add_callback(self.recv_right, [write_stream])

    # TODO: use a callback on a stateful read stream instead of passing self
    def recv_left(self, msg, write_stream):
        print("PermissiveRecentJoinOp: received {msg} on left stream".format(msg=msg))
        self.left_msgs.append(msg)
        self.left_reccnt += 1
        self.send_joined_left(msg, write_stream)

    def recv_right(self, msg, write_stream):
        print("PermissiveRecentJoinOp: received {msg} on right stream".format(msg=msg))
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
        index = 0
        for right_msg in self.right_msgs:
            self.right_duplcnt += 1
            timestamp = msg.timestamp
            joined_msg = erdos.Message(timestamp, (msg.data[0], msg.timestamp, right_msg.data[0], right_msg.timestamp))
            self.count_and_log()
            print("PermissiveRecentJoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
            write_stream.send(joined_msg)
            if right_msg.timestamp < msg.timestamp:
                index += 1
        if index < len(self.right_msgs):
            self.right_msgs = self.right_msgs[index:]

    def send_joined_right(self, msg, write_stream):
        self.right_usedcnt += 1
        for left_msg in self.left_msgs:
            self.left_duplcnt += 1
            timestamp = msg.timestamp
            joined_msg = erdos.Message(timestamp, (left_msg.data[0], left_msg.timestamp, msg.data[0], msg.timestamp))
            self.count_and_log()
            print("PermissiveRecentJoinOp: sending {joined_msg}".format(joined_msg=joined_msg))
            write_stream.send(joined_msg)
            if left_msg.timestamp < msg.timestamp:
                index += 1
        if index < len(self.left_msgs):
            self.left_msgs = self.left_msgs[index:]

    @staticmethod
    def connect(left_stream, right_stream):
        return [erdos.WriteStream()]


class MeasurementOp(erdos.Operator):
    def __init__(self, read_stream, write_stream, log_file):
        read_stream.add_callback(self.callback, [write_stream])
        self.logger = utils.setup_csv_logging("time data",
                                              log_file=log_file)
        self.logger.warning("timestamp, time_difference, left_recency, right_recency")

    def callback(self, msg, write_stream):
        current_time = time.time()

        print("MeasurementOp: receiving {msg}".format(msg=msg))
        left_time, right_time = msg.data[0], msg.data[2]
        
        timestamp = max(left_time, right_time)
        difference = abs(left_time-right_time)
        left_recency = abs(left_time-current_time)
        right_recency = abs(right_time-current_time)
        data = (timestamp, difference, left_recency, right_recency)

        msg = erdos.Message(msg.timestamp, data)
        self.logger.warning(
            "{timestamp}, {time_difference}, {left_recency}, {right_recency}".format(
                timestamp = timestamp,
                time_difference=difference,
                left_recency=left_recency,
                right_recency=right_recency
                ))

        print("MeasurementOp: sending {msg}".format(msg=msg))
        write_stream.send(msg)

    @staticmethod
    def connect(read_stream):
        return [erdos.WriteStream()]


def run_experiment(join_type, left_freq, right_freq, duration_secs=100):
    if join_type == "MostPermissive":
        join = MostPermissiveJoinOp
    elif join_type == "Timestamp":
        join = TimestampJoinOp
    elif join_type == "RecentNoDupl":
        join = RecentNoDuplJoinOp
    elif join_type == "PermissiveRecent":
        join = PermissiveRecentJoinOp

    join_log_file = join_type + "counts" + "_leftfreq_" + str(left_freq) + "_rightfreq_" + str(right_freq) + "_duration_" + str(duration_secs)
    measurement_log_file = join_type + "times" + "_leftfreq_" + str(left_freq) + "_rightfreq_" + str(right_freq) + "_duration_" + str(duration_secs)

    """Creates and runs the dataflow graph."""
    (left_stream, ) = erdos.connect(SendOp,
                                    erdos.OperatorConfig(name="LeftSendOp"), [],
                                    frequency=left_freq, duration=duration_secs)
    (right_stream, ) = erdos.connect(SendOp,
                                     erdos.OperatorConfig(name="RightSendOp"), [],
                                     frequency=right_freq, duration=duration_secs)
    (join_stream, ) = erdos.connect(join,
                                    erdos.OperatorConfig(),
                                    [left_stream, right_stream], log_file=join_log_file)
    (time_stream, ) = erdos.connect(MeasurementOp,
                                    erdos.OperatorConfig(), [join_stream], log_file=measurement_log_file)
    erdos.run()


def main():
    join_types = ["Timestamp", "RecentNoDupl", "PermissiveRecent", "MostPermissive"]
    for join_type in join_types:
        for left_freq in range(1, 11):
            for right_freq in range(left_freq, 11):
                # p = Process(target=run_experiment, args=(join_type, left_freq, right_freq))
                # p.start()
                # time.sleep(12)
                # for child in p.children(recursive=True):
                #     child.kill()
                # TO DO: spawn dataflow graph creation process, then terminate it and its children processes


if __name__ == "__main__":
    main()
