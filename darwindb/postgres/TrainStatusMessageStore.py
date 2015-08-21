from darwindb.postgres.BaseStore import BaseStore
from darwindb.postgres.ScheduleMessageStore import ScheduleMessageStore

from datetime import datetime, date, time, timedelta
from dateutil.parser import parse

from collections import OrderedDict

import pytz

def subtract_times(a, b):
    delta = datetime.combine(date.today(), a) - datetime.combine(date.today(), b)
    return (delta.days*24*60*60)+delta.seconds

class TrainStatusMessageStore(BaseStore):
    
    def __init__(self, connection):
        super().__init__(connection)
        s = ScheduleMessageStore

        self.update_schedule_query = "UPDATE {} SET {} WHERE {}".format(
                s.table_schedule_name,
                ", ".join(["{}=%s".format(k) for k, v in list(s.table_schedule_fields.items())[15:]]),
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

        self.update_point_prepare = "PREPARE ts_update_point as UPDATE {} SET {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} WHERE {}".format(
                s.table_schedule_location_name,
                "suppressed=$1",
                "forecast_arrival_estimated_time=$2",
                "forecast_arrival_working_estimated_time=$3",
                "forecast_arrival_actual_time=$4",
                "forecast_arrival_actual_time_removed=$5",
                "forecast_arrival_manual_estimate_lower_limit=$6",
                "forecast_arrival_manual_estimate_unknown_delay=$7",
                "forecast_arrival_unknown_delay=$8",
                "forecast_arrival_source=$9",
                "forecast_arrival_source_cis=$10",
                "forecast_pass_estimated_time=$11",
                "forecast_pass_working_estimated_time=$12",
                "forecast_pass_actual_time=$13",
                "forecast_pass_actual_time_removed=$14",
                "forecast_pass_manual_estimate_lower_limit=$15",
                "forecast_pass_manual_estimate_unknown_delay=$16",
                "forecast_pass_unknown_delay=$17",
                "forecast_pass_source=$18",
                "forecast_pass_source_cis=$19",
                "forecast_departure_estimated_time=$20",
                "forecast_departure_working_estimated_time=$21",
                "forecast_departure_actual_time=$22",
                "forecast_departure_actual_time_removed=$23",
                "forecast_departure_manual_estimate_lower_limit=$24",
                "forecast_departure_manual_estimate_unknown_delay=$25",
                "forecast_departure_unknown_delay=$26",
                "forecast_departure_source=$27",
                "forecast_departure_source_cis=$28",
                "id=$29")

        self.select_tz_prepare = "PREPARE ts_select_tz as SELECT {} from {} WHERE {}".format(
                "timezone",
                s.table_schedule_name,
                "rid=$1")

        self.select_tz_execute = "EXECUTE ts_select_tz (%s)"

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
            self.cursor.execute(self.select_tz_prepare)

        self.cursor.execute("EXECUTE ts_select_points (%s)", (message["rid"],))

        if self.cursor.rowcount == 0:
            print("--- Cannot apply because we don't have the relevant schedule record yet.")
        else:
            print("+++ Schedule record is present. Can apply.")

            # Get the rows from that query.
            rows = self.cursor.fetchall()

            # Get the timezone to apply the schedule to.
            self.cursor.execute(self.select_tz_execute, (message["rid"],))
            if self.cursor.rowcount != 1:
                print("ERROR Row Count not equal to one when getting timezone. This should be impossible.")
                return

            tz = pytz.timezone(self.cursor.fetchall()[0][0])

            for m in message["locations"]:
                found = False
                for r in rows:
                    #print("{} : {} : {} : {}".format(r[1], r[7], r[9], r[11]))
                    if r[1] == m["tiploc"] and \
                    r[7] == m["raw_working_arrival_time"] and \
                    r[9] == m["raw_working_pass_time"] and \
                    r[11] == m["raw_working_departure_time"]:
                        #print("    +++ Found matching row.")
                        found = True
                        
                        # Now figure out the times that belong in the forecasts.
                        # For each time work out if the date has progressed.
                        arrival_working_estimated_time = None
                        if r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("working_estimated_time", None) is not None:
                                arrival_working_estimated_time = self.determine_stuff(r[2], tz, r[7], parse(m["arrival"]["working_estimated_time"]).time())
                        
                        arrival_estimated_time = None
                        if r[8] is not None:
                            if m["arrival"] is not None and m["arrival"].get("estimated_time", None) is not None:
                                arrival_estimated_time = self.determine_stuff(r[3], tz, r[8], parse(m["arrival"]["estimated_time"]).time())
                        elif r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("estimated_time", None) is not None:
                                arrival_estimated_time = self.determine_stuff(r[2], tz, r[7], parse(m["arrival"]["estimated_time"]).time())
                        
                        arrival_actual_time = None
                        if r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("actual_time", None) is not None:
                                arrival_actual_time = self.determine_stuff(r[2], tz, r[7], parse(m["arrival"]["actual_time"]).time())

                        arrival_manual_estimate_lower_limit = None
                        if r[7] is not None:
                            if m["arrival"] is not None and m["arrival"].get("manual_estimate_lower_limit_minutes", None) is not None:
                                arrival_manual_estimate_lower_limit = self.determine_stuff(r[2], tz, r[7], parse(m["arrival"]["manual_estimate_lower_limit_minutes"]).time())

                        arrival_actual_time_removed = None
                        arrival_manual_estimate_unknown_delay = None
                        arrival_unknown_delay = None
                        arrival_source = None
                        arrival_source_cis = None

                        if m.get("arrival", None) is not None:
                            arrival_actual_time_removed = m["arrival"].get("actual_time_removed", None)
                            arrival_manual_estimate_unknown_delay = m["arrival"].get("manual_estimate_unknown_delay", None)
                            arrival_unknown_delay = m["arrival"].get("unknown_delay", None)
                            arrival_source = m["arrival"].get("source", None)
                            arrival_source_cis = m["arrival"].get("source_cis", None)


                        pass_working_estimated_time = None
                        if r[9] is not None:
                            if m["pass"] is not None and m["pass"].get("working_estimated_time", None) is not None:
                                pass_working_estimated_time = self.determine_stuff(r[4], tz, r[9], parse(m["pass"]["working_estimated_time"]).time())
                        
                        pass_estimated_time = None
                        if r[9] is not None:
                            if m["pass"] is not None and m["pass"].get("estimated_time", None) is not None:
                                pass_estimated_time = self.determine_stuff(r[4], tz, r[9], parse(m["pass"]["estimated_time"]).time())
                        
                        pass_actual_time = None
                        if r[9] is not None:
                            if m["pass"] is not None and m["pass"].get("actual_time", None) is not None:
                                pass_actual_time = self.determine_stuff(r[4], tz, r[9], parse(m["pass"]["actual_time"]).time())

                        pass_manual_estimate_lower_limit = None
                        if r[9] is not None:
                            if m["pass"] is not None and m["pass"].get("manual_estimate_lower_limit_minutes", None) is not None:
                                pass_manual_estimate_lower_limit = self.determine_stuff(r[4], tz, r[9], parse(m["pass"]["manual_estimate_lower_limit_minutes"]).time())

                        pass_actual_time_removed = None
                        pass_manual_estimate_unknown_delay = None
                        pass_unknown_delay = None
                        pass_source = None
                        pass_source_cis = None

                        if m.get("pass", None) is not None:
                            pass_actual_time_removed = m["pass"].get("actual_time_removed", None)
                            pass_manual_estimate_unknown_delay = m["pass"].get("manual_estimate_unknown_delay", None)
                            pass_unknown_delay = m["pass"].get("unknown_delay", None)
                            pass_source = m["pass"].get("source", None)
                            pass_source_cis = m["pass"].get("source_cis", None)


                        departure_working_estimated_time = None
                        if r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("working_estimated_time", None) is not None:
                                departure_working_estimated_time = self.determine_stuff(r[2], tz, r[7], parse(m["departure"]["working_estimated_time"]).time())
                        
                        departure_estimated_time = None
                        if r[8] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("estimated_time", None) is not None:
                                departure_estimated_time = self.determine_stuff(r[3], tz, r[8], parse(m["departure"]["estimated_time"]).time())
                        elif r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("estimated_time", None) is not None:
                                departure_estimated_time = self.determine_stuff(r[2], tz, r[7], parse(m["departure"]["estimated_time"]).time())
                        
                        departure_actual_time = None
                        if r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("actual_time", None) is not None:
                                departure_actual_time = self.determine_stuff(r[2], tz, r[7], parse(m["departure"]["actual_time"]).time())

                        departure_manual_estimate_lower_limit = None
                        if r[7] is not None:
                            if m.get("departure", None) is not None and m["departure"].get("manual_estimate_lower_limit_minutes", None) is not None:
                                departure_manual_estimate_lower_limit = self.determine_stuff(r[2], tz, r[7], parse(m["departure"]["manual_estimate_lower_limit_minutes"]).time())

                        departure_actual_time_removed = None
                        departure_manual_estimate_unknown_delay = None
                        departure_unknown_delay = None
                        departure_source = None
                        departure_source_cis = None

                        if m.get("departure", None) is not None:
                            departure_actual_time_removed = m["departure"].get("actual_time_removed", None)
                            departure_manual_estimate_unknown_delay = m["departure"].get("manual_estimate_unknown_delay", None)
                            departure_unknown_delay = m["departure"].get("unknown_delay", None)
                            departure_source = m["departure"].get("source", None)
                            departure_source_cis = m["departure"].get("source_cis", None)


                        self.cursor.execute("EXECUTE ts_update_point (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                            None if "suppressed" not in m else m["suppressed"],
                            arrival_estimated_time,
                            arrival_working_estimated_time,
                            arrival_actual_time,
                            arrival_actual_time_removed,
                            arrival_manual_estimate_lower_limit,
                            arrival_manual_estimate_unknown_delay,
                            arrival_unknown_delay,
                            arrival_source,
                            arrival_source_cis,
                            pass_estimated_time,
                            pass_working_estimated_time,
                            pass_actual_time,
                            pass_actual_time_removed,
                            pass_manual_estimate_lower_limit,
                            pass_manual_estimate_unknown_delay,
                            pass_unknown_delay,
                            pass_source,
                            pass_source_cis,
                            departure_estimated_time,
                            departure_working_estimated_time,
                            departure_actual_time,
                            departure_actual_time_removed,
                            departure_manual_estimate_lower_limit,
                            departure_manual_estimate_unknown_delay,
                            departure_unknown_delay,
                            departure_source,
                            departure_source_cis,
                            r[0],
                        ))
                        break
                if not found:
                    print("    --- Did not find matching row.")
                    pass
            
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

    def determine_stuff(self, dated_schedule_time, tz, schedule_time, forecast_time):
        delta = subtract_times(forecast_time, schedule_time)
        if delta < -21600:
            # Crossed Midnight into the next day.
            d = (dated_schedule_time + timedelta(days=1)).date()

        elif delta < 64800:
            # Normal time.
            d = dated_schedule_time.date()

        else:
            # Delayed backwards over midnight into previous day.
            d = (dated_schedule_time + timedelta(days=-1)).date()

        return tz.localize(datetime.combine(d, forecast_time)).astimezone(pytz.utc)


