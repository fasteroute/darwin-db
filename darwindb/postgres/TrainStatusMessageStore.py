from darwindb.postgres.BaseStore import BaseStore
from darwindb.postgres.ScheduleMessageStore import ScheduleMessageStore

from dateutil.parser import parse

from collections import OrderedDict

class TrainStatusMessageStore(BaseStore):
    
    def __init__(self, connection):
        super().__init__(connection)
        s = ScheduleMessageStore

        self.update_schedule_query = "UPDATE {} SET {} WHERE {}".format(
                s.table_schedule_name,
                ", ".join(["{}=%s".format(k) for k, v in list(s.table_schedule_fields.items())[14:]]),
                "rid=%s")

        self.select_location_query_prepare = "PREPARE ts_select_location_query as SELECT {} from {} WHERE {}".format(
                "id, route_delay, working_arrival_time, public_arrival_time, working_pass_time, public_departure_time, working_departure_time",
                s.table_schedule_location_name,
                " and ".join(["{} IS NOT DISTINCT FROM ${}".format(k, idx+1) for idx, k in enumerate([
                    "rid",
                    "tiploc",
                    "raw_working_arrival_time",
                    #"raw_public_arrival_time",
                    "raw_working_pass_time",
                    #"raw_public_departure_time",
                    "raw_working_departure_time"
                ])]))
        self.select_location_query_execute = "EXECUTE ts_select_location_query (%s, %s, %s, %s, %s)"

        self.select_points_prepare = "PREPARE ts_select_points as SELECT {} from {} WHERE {}".format(
                "id, tiploc, working_arrival_time, public_arrival_time, working_pass_time, public_departure_time, working_departure_time, raw_working_arrival_time, raw_public_arrival_time, raw_working_pass_time, raw_public_departure_time, raw_working_departure_time",
                s.table_schedule_location_name,
                "rid=$1")

        self.update_point_prepare = "PREPARE ts_update_point as UPDATE {} SET {} WHERE {}".format(
                s.table_schedule_location_name,
                "suppressed=$1",
                "id=$2")

        self.cursor = None

    def create_tables(self):
        # No tables to create - the ScheduleMessageStore is responsible for that.
        pass

    def save_train_status_message(self, message):
        # Prepare message
        self.prepare_message(message)

        # Check the train concerned is in the database.
        if self.cursor is None:
            self.cursor = self.connection.cursor()
#            self.cursor.execute("PREPARE get_schedule_by_rid_count as SELECT rid FROM schedule WHERE rid=$1")
#            self.cursor.execute(self.select_location_query_prepare)
            self.cursor.execute(self.select_points_prepare)
            self.cursor.execute(self.update_point_prepare)

        self.cursor.execute("EXECUTE ts_select_points (%s)", (message["rid"],))

        if self.cursor.rowcount == 0:
            print("--- Cannot apply because we don't have the relevant schedule record yet.")
        else:
            print("+++ Schedule record is present. Can apply.")

            rows = self.cursor.fetchall()
            for r in rows:
                found = False
                for m in message["locations"]:
                    #print("{} : {} : {} : {}".format(r[1], r[7], r[9], r[11]))
                    if r[1] == m["tiploc"] and \
                    r[7] == m["raw_working_arrival_time"] and \
                    r[9] == m["raw_working_pass_time"] and \
                    r[11] == m["raw_working_departure_time"]:
                        #print("    +++ Found matching row.")
                        found = True
                        self.cursor.execute("EXECUTE ts_update_point (%s, %s)", (None if "suppressed" not in m else m["suppressed"], r[0],))
                        break
                if not found:
                    #print("    --- Did not find matching row.")
                    pass
            
#        self.cursor.execute("EXECUTE get_schedule_by_rid_count (%s)", (message['rid'],))
#
#        if self.cursor.rowcount != 1:
#            print("--- Cannot apply because we don't have the relevant schedule record yet.")
#            pass
#        else:
#            print("+++ Schedule record is present. Can apply.")
#            if message.get("late_reason", None) is None:
#                late_reason_code = None
#                late_reason_tiploc = None
#                late_reason_near = None
#            else:
#                late_reason_code = message["late_reason"].get("code", None)
#                late_reason_tiploc = message["late_reason"].get("tiploc", None)
#                late_reason_near = message["late_reason"].get("near", None)
#
#            self.cursor.execute(self.update_schedule_query, (
#                message["reverse_formation"],
#                late_reason_code,
#                late_reason_tiploc,
#                late_reason_near,
#                message["rid"],
#            ))
#
#            for l in message["locations"]:
#                self.cursor.execute(self.select_location_query_execute, (
#                    message["rid"],
#                    l["tiploc"],
#                    l.get("working_arrival_time", None),
#                    #l.public_arrival_time,
#                    l.get("working_pass_time", None),
#                    #l.public_departure_time,
#                    l.get("working_departure_time", None),
#                ))
#                if self.cursor.rowcount == 0:
#                    print("   --- 0 Matching Schedule locations found.")
#                    # TODO: Figure out wtf is the cause of this.
#                    pass
#                elif self.cursor.rowcount == 1:
#                    #print("   +++ 1 Matching Schedule location found.")
#                    # TODO: Calculate the time. We do this by selecting the first schedule_location
#                    #       of the schedule this location is part of (do this outside the loop to
#                    #       avoid spurious queries) and using that and the start_date to figure out
#                    #       the timezone which should be applied to all times.
#                    #
#                    #       Next we compare the raw times from the status message and the schedule
#                    #       location record to work out if we have gone forward or backward in time
#                    #       and if the date has changed, as per the Darwin rules on the wiki.
#                    #
#                    #       Then we should be able to infer the actual UTC time and the appropriate
#                    #       date to apply to all the forecast times, and can then save them to
#                    #       the database.
#                    pass
#                else:
#                    print("   !!! {} matching schedule locations found.".format(cursor.rowcount))
#                    # TODO: We seem to be getting some duplicate calling points in the database,
#                    #       with exactly the same times and tiplocs. Is this a bug in the Schedule
#                    #       message processing code, or is this a bug in the data, or is there
#                    #       something important in how the data works that I'm missing?
#                    pass
#
        self.connection.commit()

    def prepare_message(self, message):

        for l in message["locations"]:
            if "working_arrival_time" in l:
                l["raw_working_arrival_time"] = parse(l["working_arrival_time"]).time()
            else:
                l["raw_working_arrival_time"] = None

            if "working_pass_time" in l:
                l["raw_working_pass_time"] = parse(l["working_pass_time"]).time()
            else:
                l["raw_working_pass_time"] = None

            if "working_departure_time" in l:
                l["raw_working_departure_time"] = parse(l["working_departure_time"]).time()
            else:
                l["raw_working_departure_time"] = None

            if "public_arrival_time" in l:
                l["raw_public_arrival_time"] = parse(l["public_arrival_time"]).time()
            else:
                l["raw_public_arrival_time"] = None

            if "public_departure_time" in l:
                l["raw_public_departure_time"] = parse(l["public_departure_time"]).time()
            else:
                l["raw_public_departure_time"] = None


