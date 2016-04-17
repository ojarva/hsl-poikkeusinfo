# encoding=utf-8

"""
Classes for downloading, parsing and filtering poikkeusinfo.fi xml files.
"""

from local_settings import LINES, FETCH_INTERVAL
import datetime
import glob
import json
import pprint
import re
import redis
import requests
import time
import xmltodict
import pytz
import logging


class DateTimeEncoder(json.JSONEncoder):
    """ Encodes items with datetime objects properly """

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)


class PoikkeusInfoParser(object):
    """ Parses poikkeusinfo XML.

    See http://developer.reittiopas.fi/media/Poikkeusinfo_XML_rajapinta_V2_2_01.pdf
    """

    # Different time formats for estimated lengths
    TIME_RE = [
        re.compile(r"^(?P<start_time>([0-9]{1,2}:[0-9]{2})|([0-9]{1,2}))\s*-\s*(?P<end_date>[0-9]{1,2}\.[0-9]{2})\.{0,1}\s*(klo|kello)\.*\s*(?P<end_time>([0-9]{2}:[0-9]{2})|([0-9]{1,2}))"),
        re.compile(r"^(?P<end_time>([0-9]{1,2}:[0-9]{2})|([0-9]{1,2}))\s*(asti|)(\.|)$"),
        re.compile(r"^(?P<start_time>([0-9]{1,2}:[0-9]{2})|([0-9]{1,2}))\s*-\s*(?P<end_time>([0-9]{1,2}:[0-9]{2})|([0-9]{1,2}))"),
    ]

    # Formats for estimated length dates
    DATE_FORMATS = [
        "%d.%m",
        "%d.%m.",
        "%d.%m.%Y",
        "%d..%m",
        "%d..%m.%Y",
    ]

    # Formats for estimated length times
    TIME_FORMATS = [
        "%H:%M",
        "%H",
    ]

    # Mapping for notification types (see pdf)
    TYPE_MAP = {
        "1": "advance_info",
        "2": "urgent_info",
    }

    # Mapping for notification sources (see pdf)
    SOURCE_MAP = {
        "1": "manual",  # manually entered
        "2": "automatic",  # automatically imported from other HSL systems
    }

    # Mapping for line types (see pdf)
    LINETYPE_MAP = {
        "1": "helsinki",
        "2": "tram",
        "3": "espoo",
        "4": "vantaa",
        "5": "regional_traffic",
        "6": "metro",
        "7": "ferry",
        "12": "train",
        "14": "all",
        "36": "kirkkonummi",
        "39": "kerava",
    }

    # Regex for fetching reason phrase
    REASON_RE = re.compile(r".*Syy:\s*(?P<reason>[^\.]*)\.*")

    # Mapping for departure directions (see pdf)
    DIRECTION_MAP = {
        "1": "from_centrum",
        "2": "to_centrum",
    }

    # Mapping for reasons - some typo fixes and unifying.
    REASON_MAP = {
        "Helsinki City Marathon": "yleisötapahtuma",
        "maraton": "yleisötapahtuma",
        "sambakulkue": "yleisötapahtuma",
        "liukkaus": "sääolosuhteet",
        "tien liukkaus": "sääolosuhteet",
        "Helsinki City Run": "yleisötapahtuma",
        "Vantaa Triathlon": "yleisötapahtuma",
        "lehtikelin aiheuttama liukkaus": "sääolosuhteet",
        "keliolosuhteet": "sääolosuhteet",
        "tekninen häiriö": "tekninen vika",
        "Tietyö": "tietyö",
        "Sääolosuhteet": "sääolosuhteet",
        "kulkue": "yleisötapahtuma",
        "juoksutapahtuma": "yleisötapahtuma",
        "virtahäiriö": "tekninen vika",
        "Työnseisaus": "lakko",
        "tie poikki (viranomaisten toimesta)": "tie poikki",
        "työnseisaus": "lakko",
        "tietyömaa": "tietyö",
        "työmaa": "tietyö",
        "vaihdevika": "tekninen vika radassa",
        "kiskotyöt": "ratatyöt",
        "Este tiellä": "este tiellä",
        "sääolosuhteet, ajolangat jäätyy": "sääolosuhteet",
        "väärin pysäköidyt autot": "väärin pysäköity auto",
        "väärin pysäköity auito": "väärin pysäköity auto",
    }

    def parse_length(self, reason, timestamp):
        """ Parses 'estimated length' from freetext field """

        if "Arvioitu kesto: " not in reason:
            return None

        helsinki = pytz.timezone("Europe/Helsinki")

        estimated_length = reason.split("Arvioitu kesto: ")[1]
        for regex in self.TIME_RE:
            match = regex.match(estimated_length)
            if not match:
                continue

            parsed_timestamp = datetime.datetime(1900, 1, 1)
            try:
                end_date = match.group("end_date")
                for date_format in self.DATE_FORMATS:
                    try:
                        day_part = datetime.datetime.strptime(end_date, date_format)
                        parsed_timestamp += (day_part - datetime.datetime(1900, 1, 1))
                        break
                    except ValueError:
                        pass
            except IndexError:
                parsed_timestamp += (datetime.datetime(1900, timestamp.month, timestamp.day) - datetime.datetime(1900, 1, 1))

            for time_format in self.TIME_FORMATS:
                try:
                    time_part = datetime.datetime.strptime(match.group("end_time"), time_format)
                    parsed_timestamp += (time_part - datetime.datetime(1900, 1, 1))
                    parsed_timestamp += (datetime.datetime(timestamp.year, 1, 1) - datetime.datetime(1900, 1, 1))
                    return helsinki.localize(parsed_timestamp)
                except ValueError:
                    pass
        return None

    def parse_reason(self, text):
        """ Parses reason information from freetext field, if available. Returns None if no match is found. """
        match = self.REASON_RE.match(text)
        if match:
            reason = match.group("reason").encode("utf-8").strip()
            reason = self.REASON_MAP.get(reason, reason)
            return reason
        return None

    def parse_info(self, info, timestamp):
        """ Parses info field, including length and reason """
        text_item = None
        if isinstance(info["TEXT"], list):
            for item in info["TEXT"]:
                if item["@lang"] == "fi":
                    text_item = item
                    break
        elif "TEXT" in info:
            text_item = info["TEXT"]
        if text_item:
            if "#text" in text_item:
                data = {
                    "length": self.parse_length(text_item["#text"], timestamp),
                    "reason": self.parse_reason(text_item["#text"]),
                    "text": text_item["#text"],
                }
                return data
        return None

    def parse_targets(self, targets):
        """ Parses targets (affected lines) """
        if targets is None:
            return None
        lines = []
        for k, target in targets.items():
            if k == "LINE":
                if not isinstance(target, list):
                    target = [target]
                for line in target:
                    lines.append({"id": line["@id"], "direction": self.DIRECTION_MAP.get(line["@direction"]), "type": self.LINETYPE_MAP.get(line["@linetype"]), "number": line["#text"]})
        return lines

    @classmethod
    def parse_isoformat(cls, time_string):
        """ Parses ISO-8601 datetimes (without timezone) to python datetime """
        helsinki = pytz.timezone("Europe/Helsinki")
        return helsinki.localize(datetime.datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S"))

    def parse_validity(self, validity):
        """ Parses notification validity timestamps and "valid" tag.

        If "valid" is False, notification should be hidden from the user. """
        data = {
            "valid": validity["@status"] == "1",
            "from": self.parse_isoformat(validity["@from"]),
            "to": self.parse_isoformat(validity["@to"]),
        }
        return data

    def parse_item(self, item, timestamp):
        """ Parses a single deserialized item """
        data = {
            "id": item["@id"],
            "type": self.TYPE_MAP[item["@type"]],
            "source": self.SOURCE_MAP[item["@source"]],
            "info": self.parse_info(item["INFO"], timestamp),
            "lines": self.parse_targets(item["TARGETS"]),
            "validity": self.parse_validity(item["VALIDITY"]),
        }
        return data

    def parse(self, content, timestamp):
        """ Parses XML from poikkeusinfo.fi """
        parsed = xmltodict.parse(content)
        if "DISRUPTIONS" not in parsed:
            return
        parsed = parsed["DISRUPTIONS"]
        items = []
        if "DISRUPTION" in parsed:
            disruptions = parsed["DISRUPTION"]
            if not isinstance(disruptions, list):
                disruptions = [disruptions]
            for item in disruptions:
                items.append(self.parse_item(item, timestamp))
        return items


class PoikkeusInfoFilter(object):
    """ Filters entries based on configuration dictionary. """

    def __init__(self, config):
        self.config = config

    def filter_item(self, item):
        """ Checks whether a single item should be included. Returns either None or item """

        if not item["validity"]["valid"] or item["lines"] is None:
            return None
        for line_name, config in self.config.items():
            for filter_by in item["lines"]:
                if "line_type" in config and filter_by["type"] != config["line_type"]:
                    continue
                if "directions" in config and filter_by["direction"] not in config["directions"]:
                    continue
                if "numbers" in config and filter_by["number"] not in config["numbers"]:
                    continue
                item["display_name"] = line_name
                return item

    def filter(self, lines):
        """ Filters a list of items. """

        filtered_lines = []
        for line in lines:
            filtered = self.filter_item(line)
            if filtered:
                filtered_lines.append(filtered)
        return filtered_lines


class PoikkeusInfoRunner(object):
    """ Fetches XML from poikkeusinfo.fi, parse, filters and publishes to redis.

    Fetch interval is configured by FETCH_INTERVAL variable.
     """

    def __init__(self):
        self.pip = PoikkeusInfoParser()
        self.pif = PoikkeusInfoFilter(LINES)
        self.redis_instance = redis.StrictRedis()
        self.last_run_at = None
        self.logger = logging.getLogger("poikkeusinfo-runner")
        self.logger.setLevel(logging.INFO)
        format_string = "%(asctime)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(format_string)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def fetch(self):
        """ A single fetch. Returns False on failure. Saves and publishes updates to redis. """

        resp = requests.get("http://www.poikkeusinfo.fi/xml/v2/fi")
        if resp.status_code != 200:
            self.logger.info("Fetching failed with status code %s", resp.status_code)
            return False
        parsed = self.pip.parse(resp.content, datetime.datetime.now())
        filtered = self.pif.filter(parsed)
        dumped = json.dumps(filtered, cls=DateTimeEncoder)
        self.redis_instance.setex("hsl-poikkeusinfo", 3600, dumped)
        self.redis_instance.publish("home:broadcast:generic", json.dumps({"key": "poikkeusinfo", "content": filtered}, cls=DateTimeEncoder))
        return filtered

    def run(self):
        """ Runner for periodic fetching and publishing. Configure interval with FETCH_INTERVAL variable. """
        self.last_run_at = time.time()
        while True:
            self.logger.info("Starting")
            self.fetch()
            sleep_time = max(FETCH_INTERVAL / 2, FETCH_INTERVAL - (time.time() - self.last_run_at))
            self.logger.info("Sleeping %ss", sleep_time)
            time.sleep(sleep_time)


def main_testing():
    """ Runs all .xml files and prints the results """
    pip = PoikkeusInfoParser()
    pif = PoikkeusInfoFilter(LINES)
    for filename in glob.glob("*.xml"):
        timestamp = datetime.datetime.now()
        content = open(filename).read()
        parsed = pip.parse(content, timestamp)
        filtered = pif.filter(parsed)
        if len(filtered) > 0:
            pprint.pprint(filtered)


def main_run():
    """ Starts periodic download/parse/filter/publish cycle in foreground """
    pir = PoikkeusInfoRunner()
    pir.run()

if __name__ == '__main__':
    main_run()
