#!/usr/bin/python3

import argparse
import string
import datetime
import time
import calendar
import socket
import redis
import signal
import hashlib
import json
import uuid
import jsonschema
import threading

# define some global variables
class t_global(object):
    alarm_active = False
    args = None
    con_pool = None
    con_pool_state = False
    con_watchdog_exit = None
    con_watchdog = None
    redcon = None
    pubsubcon = None
    initiator = False
    mirror_busB = False
    schema = None
    user_schema = None
    my_id = None
    watch_busA = True
    watch_busB = False
    leader_abort = False
    follower_abort = False
    initiator_type = None
    initiator_id = None
    followers = { "online": {},
                  "ready": {},
                  "gone": {} }
    processed_messages = {}
    messages = { "sent": [],
                 "received": [] }
    message_log = None
    user_messages = None

def debug(log_msg):
    return(print("DEBUG: %s" % (log_msg)))

def message_to_str(message):
    return(json.dumps(message, separators=(",", ":")))

def message_from_str(message):
    return(json.loads(message))

def message_build(recipient_type, recipient_id, command, value=None):
    return(message_build_custom(t_global.args.roadblock_role, t_global.my_id, recipient_type, recipient_id, command, value))

def message_build_custom(sender_type, sender_id, recipient_type, recipient_id, command, value=None):
    message = {
        "payload": {
            "uuid": str(uuid.uuid4()),
            "roadblock": t_global.args.roadblock_uuid,
            "sender": {
                "timestamp": calendar.timegm(time.gmtime()),
                "type": sender_type,
                "id": sender_id,
            },
            "recipient": {
                "type": recipient_type
            },
            "message": {
                "command": command
            }
        },
        "checksum": None
    }

    if recipient_type != "all":
        message["payload"]["recipient"]["id"] = recipient_id

    if value is not None:
        if command == "user-string":
            message["payload"]["message"]["user-string"] = value
        elif command == "user-object":
            message["payload"]["message"]["user-object"] = value
        else:
            message["payload"]["message"]["value"] = str(value)

    message["checksum"] = hashlib.sha256(str(message_to_str(message["payload"])).encode("utf-8")).hexdigest()

    return(message)

def message_validate(message):
    try:
        jsonschema.validate(instance=message, schema=t_global.schema)

        checksum = hashlib.sha256(str(message_to_str(message["payload"])).encode("utf-8")).hexdigest()

        if message["checksum"] == checksum:
            return(True)
        else:
            return(False)
    except:
        return(False)

def message_for_me(message):
    message["payload"]["recipient"]["timestamp"] = calendar.timegm(time.gmtime())

    if message["payload"]["sender"]["id"] == t_global.my_id and message["payload"]["sender"]["type"] == t_global.args.roadblock_role:
        # I'm the sender so ignore it
        return(False)
    elif message["payload"]["recipient"]["type"] == "all":
        return(True)
    elif message["payload"]["recipient"]["type"] == t_global.args.roadblock_role and message["payload"]["recipient"]["id"] == t_global.my_id:
        return(True)
    else:
        return(False)

def message_get_command(message):
    return(message["payload"]["message"]["command"])

def message_get_value(message):
    return(message["payload"]["message"]["value"])

def message_get_sender(message):
    return(message["payload"]["sender"]["id"])

def message_get_sender_type(message):
    return(message["payload"]["sender"]["type"])

def message_get_uuid(message):
    return(message["payload"]["uuid"])

def define_usr_msg_schema():
    t_global.user_schema = {
        "type": "array",
        "minItems": 1,
        "uniqueItems": True,
        "items": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "recipient": {
                            "$ref": "#/definitions/recipient"
                        },
                        "user-string": {
                            "type": "string",
                            "minLength": 1
                        }
                    },
                    "required": [
                        "recipient",
                        "user-string"
                    ],
                    "additionalProperties": False
                },
                {
                    "type": "object",
                    "properties": {
                        "recipient": {
                            "$ref": "#/definitions/recipient"
                        },
                        "user-object": {
                            "type": "object"
                        }
                    },
                    "required": [
                        "recipient",
                        "user-object"
                    ],
                    "additionalProperties": False
                }
            ]
        },
        "definitions": {
            "recipient": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": [
                            "leader",
                            "follower",
                            "all"
                        ]
                    },
                    "id": {
                        "type": "string",
                        "minLength": 1
                    }
                },
                "required": [
                    "type",
                    "id"
                ],
                "additionalProperties": False
            }
        }
    }

def define_msg_schema():
    t_global.schema = {
        "type": "object",
        "properties": {
            "payload": {
                "type": "object",
                "properties": {
                    "uuid": {
                        "type": "string",
                        "minLength": 36,
                        "maxLength": 36
                    },
                    "roadblock": {
                        "type": "string",
                        "enum": [
                            t_global.args.roadblock_uuid
                        ]
                    },
                    "sender": {
                        "type": "object",
                        "properties": {
                            "timestamp": {
                                "type": "integer"
                            },
                            "type": {
                                "type": "string",
                                "enum": [
                                    "leader",
                                    "follower"
                                ]
                            },
                            "id": {
                                "type": "string",
                                "minLength": 1
                            }
                        },
                        "required": [
                            "timestamp",
                            "type",
                            "id"
                        ],
                        "additionalProperties": False
                    },
                    "recipient": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [
                                    "leader",
                                    "follower",
                                    "all"
                                ]
                            },
                            "id": {
                                "type": "string",
                                "minLength": 1
                            }
                        },
                        "required": [
                            "type"
                        ],
                        "additionalProperties": False,
                        "if": {
                            "properties": {
                                "type": {
                                    "enum": [
                                        "leader",
                                        "follower"
                                    ]
                                }
                            }
                        },
                        "then": {
                            "required": [
                                "id"
                            ]
                        }
                    },
                    "message": {
                        "type": "object",
                        "properties": {
                            "command": {
                                "type": "string",
                                "enum": [
                                    "timeout-ts",
                                    "initialized",
                                    "switch-buses",
                                    "leader-online",
                                    "follower-online",
                                    "all-online",
                                    "initiator-info",
                                    "follower-ready",
                                    "follower-ready-abort",
                                    "all-ready",
                                    "all-go",
                                    "all-abort",
                                    "follower-gone",
                                    "all-gone",
                                    "user-string",
                                    "user-object"
                                ]
                            },
                            "value": {
                                "type": "string",
                                "minLength": 1
                            },
                            "user-string": {
                                "type": "string",
                                "minLength": 1
                            },
                            "user-object": {}
                        },
                        "required": [
                            "command"
                        ],
                        "additionalProperties": False,
                        "allOf": [
                            {
                                "if": {
                                    "properties": {
                                        "command": {
                                            "enum": [
                                                "timeout-ts"
                                            ]
                                        }
                                    }
                                },
                                "then": {
                                    "required": [
                                        "value"
                                    ]
                                }
                            },
                            {
                                "if": {
                                    "properties": {
                                        "command": {
                                            "enum": [
                                                "user-string"
                                            ]
                                        }
                                    }
                                },
                                "then": {
                                    "required": [
                                        "user-string"
                                    ]
                                }
                            },
                            {
                                "if": {
                                    "properties": {
                                        "command": {
                                            "enum": [
                                                "user-object"
                                            ]
                                        }
                                    }
                                },
                                "then": {
                                    "required": [
                                        "user-object"
                                    ]
                                }
                            }
                        ]
                    }
                },
                "required": [
                    "uuid",
                    "roadblock",
                    "sender",
                    "recipient",
                    "message"
                ],
                "additionalProperties": False
            },
            "checksum": {
                "type": "string",
                "minLength": 64,
                "maxLength": 64
            }
        },
        "required": [
            "payload",
            "checksum"
        ],
        "additionalProperties": False
    }

def send_user_messages():
    if t_global.user_messages is not None:
        print("Sending user requested messages")
        user_msg_counter = 1
        for user_msg in t_global.user_messages:
            if "user-string" in user_msg:
                print("Sending user message %d: 'user-string'" % (user_msg_counter))
                message_publish(message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-string", user_msg["user-string"]))
            elif "user-object" in user_msg:
                print("Sending user message %d: 'user-object'" % (user_msg_counter))
                message_publish(message_build(user_msg["recipient"]["type"], user_msg["recipient"]["id"], "user-object", user_msg["user-object"]))

            user_msg_counter += 1

def message_handle (message):
    msg_uuid = message_get_uuid(message)
    if msg_uuid in t_global.processed_messages:
        if t_global.args.debug:
            debug("I have already processed this message! [%s]" % (msg_uuid))
        return(0)
    else:
        if t_global.args.debug:
            debug("adding uuid='%s' to the processed messages list" % (msg_uuid))
        t_global.processed_messages[msg_uuid] = True

        if t_global.message_log is not None:
            # if the message log is open then append messages to the queue
            # for later dumping
            t_global.messages["received"].append(message)

    msg_command = message_get_command(message)

    if msg_command == "timeout-ts":
        print("Received 'timeout-ts' message")

        cluster_timeout = int(message_get_value(message))

        mytime = calendar.timegm(time.gmtime())
        timeout = mytime - cluster_timeout

        if timeout < 0:
            signal.alarm(abs(timeout))
            t_global.alarm_active = True
            print("The new timeout value is in %d seconds" % (abs(timeout)))
            print("Timeout: %s" % (datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC")))
        else:
            signal.alarm(0)
            t_global.alarm_active = False
            print("The timeout has already occurred")
            return(-2)
    elif msg_command == "switch-buses":
        if t_global.args.debug:
            debug("switching busses")

        t_global.watch_busA = False
        t_global.watch_busB = True
    elif msg_command == "leader-online":
        if t_global.args.roadblock_role == "follower":
            if t_global.args.debug:
                debug("I see that the leader is online")
    elif msg_command == "follower-online":
        if t_global.args.roadblock_role == "leader":
            msg_sender = message_get_sender(message)

            if msg_sender in t_global.followers["online"]:
                print("Received 'follower-online' message from '%s'" % (msg_sender))
                del t_global.followers["online"][msg_sender]
            elif msg_sender in t_global.args.roadblock_followers:
                print("Did I already process this 'follower-online' message from follower '%s'?" % (msg_sender))
            else:
                print("Received 'follower-online' message from unknown follower '%s'" % (msg_sender))

            if len(t_global.followers["online"]) == 0:
                print("Sending 'all-online' message")
                message_publish(message_build("all", "all", "all-online"))
                if t_global.initiator:
                    t_global.mirror_busB = False
                send_user_messages()
    elif msg_command == "all-online":
        if t_global.initiator:
            print("Initiator received 'all-online' message")
            t_global.mirror_busB = False
        else:
            print("Received 'all-online' message")

        send_user_messages()

        if t_global.args.roadblock_role == "follower":
            if t_global.args.abort:
                print("Sending 'follower-ready-abort' message")
                message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-ready-abort"))
            else:
                print("Sending 'follower-ready' message")
                message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-ready"))
    elif msg_command == "follower-ready" or msg_command == "follower-ready-abort":
        if t_global.args.roadblock_role == "leader":
            if t_global.args.debug:
                debug("leader got a 'follower-ready'")

            if msg_command == "follower-ready-abort":
                t_global.leader_abort = True

            msg_sender = message_get_sender(message)

            if msg_sender in t_global.followers["ready"]:
                print("Received '%s' message from '%s'" % (msg_command, msg_sender))
                del t_global.followers["ready"][msg_sender]
            elif msg_sender in t_global.args.roadblock_followers:
                print("Received a redundant '%s' message from follower '%s'?" % (msg_command, msg_sender))
            else:
                print("Received '%s' message from unknown follower '%s'" % (msg_command, msg_sender))

            if len(t_global.followers["ready"]) == 0:
                print("Sending 'all-ready' message")
                message_publish(message_build("all", "all", "all-ready"))

                if t_global.leader_abort:
                    print("Sending 'all-abort' command")
                    message_publish(message_build("all", "all", "all-abort"))
                else:
                    print("Sending 'all-go' command")
                    message_publish(message_build("all", "all", "all-go"))
    elif msg_command == "all-ready":
        print("Received 'all-ready' message")
    elif msg_command == "all-go" or msg_command == "all-abort":
        if t_global.args.roadblock_role == "follower":
            if msg_command == "all-go":
                print("Received 'all-go' from leader")
            else:
                print("Received 'all-abort' from leader")
                t_global.follower_abort = True

            # tell the leader that I'm gone
            print("Sending 'follower-gone' message")
            message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-gone"))

            # signal myself to exit
            t_global.watch_busB = False
    elif msg_command == "follower-gone":
        if t_global.args.roadblock_role == "leader":
            if t_global.args.debug:
                debug("leader got a 'follower-gone' message")

            msg_sender = message_get_sender(message)

            if msg_sender in t_global.followers["gone"]:
                print("Received 'follower-gone' message from '%s'" % (msg_sender))
                del t_global.followers["gone"][msg_sender]
            elif msg_sender in t_global.args.roadblock_followers:
                print("Received a redundant 'follower-gone' message from follower '%s'?" % (msg_sender))
            else:
                print("Received 'follower-gone' message from unknown follower '%s'" % (msg_sender))

            if len(t_global.followers["gone"]) == 0:
                # send a message that will probably not be observed by
                # anyone...but just in case...
                print("Sending 'all-gone' message")
                message_publish(message_build("all", "all", "all-gone"))

                # signal myself to exit
                t_global.watch_busB = False
    elif msg_command == "initiator-info":
        t_global.initiator_type = message_get_sender_type(message)
        t_global.initiator_id = message_get_sender(message)
        if t_global.args.debug:
            debug("Received an 'initiator-info' message with type='%s' and id='%s'" % (t_global.initiator_type, t_global.initiator_id))

    return(0)

def message_publish(message):
    message_str = message_to_str(message)

    ret_val = 0
    counter = 0
    while ret_val == 0:
        counter += 1
        # this call should return the number of clients that receive the message
        # we expect it to be greater than zero, if not we retry
        ret_val = t_global.redcon.publish(t_global.args.roadblock_uuid + "__busB", message_str)

        if ret_val == 0:
            print("WARNING: Failed attempt %d to publish message '%s'" % (counter, message))

            backoff(counter)

    if t_global.message_log is not None:
        # if the message log is open then append messages to the queue
        # for later dumping
        t_global.messages["sent"].append(message)

    return(0)

def key_delete(key):
    ret_val = 0
    counter = 0
    while ret_val == 0:
        counter += 1
        # this call should return the number of keys deleted which is
        # expected to be one, if not we retry
        ret_val = t_global.redcon.delete(key)

        if ret_val == 0:
            print("WARNING: Failed attempt %d to delete key '%s'" % (counter, key))

            backoff(counter)

    return(0)

def key_set_once(key, value):
    ret_val = 0
    counter = 0
    while ret_val == 0:
        counter += 1
        # this call should return one on success, if not we retry
        ret_val = t_global.redcon.msetnx( { key: value } )

        if ret_val == 0:
            print("WARNING: Failed attempt %d to set key '%s' with value '%s' once" % (counter, key, value))

            backoff(counter)

    return(0)

def key_set(key, value):
    # in this case we want to return the true/false behavior so the
    # caller knows if they set the key or it already existed
    return t_global.redcon.msetnx( { key: value } )

def key_check(key):
    # inform the caller whether the key already existed or not
    return t_global.redcon.exists(key)

def list_append(key, value):
    ret_val = 0
    counter = 0
    while ret_val == 0:
        # if this call returns 0 then it failed somehow since it
        # should be the size of the list after we have added to it, so
        # we retry
        ret_val = t_global.redcon.rpush(key, value)

        if ret_val == 0:
            print("WARNING: Failed attempt %d to append value '%s' to key '%s'" % (counter, value, key))

            backoff(counter)

    return(ret_val)

def list_fetch(key, offset):
    # return the elements in the specified range (offset to end), this
    # could be empty so we can't really verify it
    return t_global.redcon.lrange(key, offset, -1)

def backoff(attempts):
    if attempts <= 10:
        # no back off, try really hard (spin)
        attempts = attempts
    elif attempts > 10 and attempts <= 50:
        # back off a bit, don't spin as quickly
        time.sleep(0.1)
    else:
        # back off more, spin even slower
        time.sleep(0.5)

    return(0)

def process_options ():
    parser = argparse.ArgumentParser(description="Roadblock provides multi entity (system, vm, container, etc.) synchronization.");

    parser.add_argument("--uuid",
                        dest = "roadblock_uuid",
                        help = "UUID that maps to the specific roadblock being processed.",
                        required = True)

    parser.add_argument("--role",
                        dest = "roadblock_role",
                        help = "What is the roadblock role of this node.",
                        default = "follower",
                        choices = ["leader", "follower"])

    parser.add_argument("--timeout",
                        dest = "roadblock_timeout",
                        help = "How long should the roadblock wait before timing out.",
                        default = 30,
                        type = int)

    parser.add_argument("--follower-id",
                        dest = "roadblock_follower_id",
                        help = "What is follower ID for this node.",
                        default = socket.getfqdn(),
                        type = str)

    parser.add_argument("--leader-id",
                        dest = "roadblock_leader_id",
                        help = "What is leader ID for this specific roadblock.",
                        type = str)

    parser.add_argument("--redis-server",
                        dest = "roadblock_redis_server",
                        help = "What is network name for the redis server (hostname or IP address).",
                        default = "localhost",
                        type = str)

    parser.add_argument("--redis-password",
                        dest = "roadblock_redis_password",
                        help = "What is password used to connect to the redis server.",
                        default = "foobar",
                        type = str)

    parser.add_argument("--followers",
                        dest = "roadblock_followers",
                        help = "Use one or more times on the leader to specify the followers by name.",
                        action = "append",
                        type = str)

    parser.add_argument("--abort",
                        dest = "abort",
                        help = "Use this option as a follower or leader to send an abort message as part of this synchronization",
                        action = "store_true")

    parser.add_argument("--debug",
                        dest = "debug",
                        help = "Turn on debug output",
                        action = "store_true")

    parser.add_argument("--message-log",
                        dest = "message_log",
                        help = "File to log all received messages to.",
                        default = None,
                        type = str)

    parser.add_argument("--user-messages",
                        dest = "user_messages",
                        help = "File to load user specified messages from.",
                        default = None,
                        type = str)

    t_global.args = parser.parse_args();


def cleanup():
    if t_global.alarm_active:
        print("Disabling timeout alarm")
        signal.alarm(0)

    if t_global.con_pool_state:
        if t_global.args.roadblock_role == "leader":
            print("Removing db objects specific to this roadblock")
            key_delete(t_global.args.roadblock_uuid)
            key_delete(t_global.args.roadblock_uuid + "__initialized")
            key_delete(t_global.args.roadblock_uuid + "__busA")

        print("Closing connection pool watchdog")
        t_global.con_watchdog_exit.set()
        t_global.con_watchdog.join()

        print("Closing connection pool")
        t_global.con_pool.disconnect()
        t_global.con_pool_state = False

    if t_global.message_log is not None:
        # if the message log is open then dump the message queue and
        # close the file handle
        print("%s\n" % (json.dumps(t_global.messages, indent = 4, separators=(',', ': '), sort_keys = False)), file=t_global.message_log)
        t_global.message_log.close()

    if t_global.args.debug:
        debug("Processed Messages:")
        for msg in t_global.processed_messages:
            debug("\t%s" % (msg))

    return(0)

def get_followers_list(followers):
    followers_list = ""

    for follower in followers:
        followers_list += follower + " "

    return(followers_list)

def do_timeout():
    print("ERROR: Roadblock failed with timeout")

    if t_global.con_pool_state and t_global.initiator:
        # set a persistent flag that the roadblock timed out so that
        # any late arriving members know that the roadblock has
        # already failed.  done by the first member since that is the
        # only member that is guaranteed to have actually reached the
        # roadblock and be capable of setting this.
        key_set_once(t_global.args.roadblock_uuid + "__timedout", int(True))

    cleanup()

    if t_global.args.roadblock_role == "leader":
        if len(t_global.followers["online"]) != 0:
            print("These followers never reached 'online': %s" % (get_followers_list(t_global.followers["online"])))
        elif len(t_global.followers["ready"]) != 0:
            print("These followers never reached 'ready': %s" % (get_followers_list(t_global.followers["ready"])))
        elif len(t_global.followers["gone"]) != 0:
            print("These followers never reach 'gone': %s" % (get_followers_list(t_global.followers["gone"])))

    exit(-3)


def sighandler(signum, frame):
    if signum == 14: # SIGALRM
        t_global.alarm_active = False
        do_timeout()
    else:
        print("Signal handler called with signal", signum)

    return(0)

def connection_watchdog():
    while not t_global.con_watchdog_exit.is_set():
        time.sleep(1)
        try:
            if t_global.con_pool_state:
                t_global.redcon.ping()
            else:
                print("ERROR: con_pool_state=False")
        except redis.exceptions.ConnectionError as con_error:
            t_global.con_pool_state = False
            print("%s" % (con_error))
            print("ERROR: Redis connection failed")

    return(0)

def main():
    process_options()

    if len(t_global.args.roadblock_leader_id) == 0:
        print("ERROR: You must specify the leader's ID using --leader-id")
        return(-1)

    if t_global.args.roadblock_role == "leader":
        if len(t_global.args.roadblock_followers) == 0:
            print("ERROR: There must be at least one follower")
            return(-1)
        if t_global.args.abort:
            t_global.leader_abort = True

        # build some hashes for easy tracking of follower status
        for follower in t_global.args.roadblock_followers:
            t_global.followers["online"][follower] = True
            t_global.followers["ready"][follower] = True
            t_global.followers["gone"][follower] = True

    if t_global.args.roadblock_role == "follower":
        t_global.my_id = t_global.args.roadblock_follower_id
    elif t_global.args.roadblock_role == "leader":
        t_global.my_id = t_global.args.roadblock_leader_id

    if t_global.args.message_log is not None:
        # open the message log, if specified
        try:
            t_global.message_log = open(t_global.args.message_log, "w")
        except IOError:
            print("ERROR: Could not open message log '%s' for writing!" % (t_global.args.message_log))
            return(-1)

    define_msg_schema()
    define_usr_msg_schema()

    if t_global.args.user_messages is not None:
        # load the user messages, if specified
        try:
            user_messages = open(t_global.args.user_messages, "r")
            t_global.user_messages = json.load(user_messages)
            user_messages.close()
        except IOError:
            print("ERROR: Could load the user messages '%s'!" % (t_global.args.user_messages))

        try:
            jsonschema.validate(instance=t_global.user_messages, schema=t_global.user_schema)
        except jsonschema.exceptions.SchemaError as e:
            print(e)
            print("ERROR: Could not JSON validate the user messages!")
            return(-1)

    # define a signal handler that will respond to SIGALRM when a
    # timeout even occurs
    signal.signal(signal.SIGALRM, sighandler)

    # set the default timeout alarm
    signal.alarm(t_global.args.roadblock_timeout)
    t_global.alarm_active = True
    mytime = calendar.timegm(time.gmtime())
    print("Current Time: %s" % (datetime.datetime.utcfromtimestamp(mytime).strftime("%Y-%m-%d at %H:%M:%S UTC")))
    cluster_timeout = mytime + t_global.args.roadblock_timeout
    print("Timeout: %s" % (datetime.datetime.utcfromtimestamp(cluster_timeout).strftime("%Y-%m-%d at %H:%M:%S UTC")))

    # create the redis connections
    while not t_global.con_pool_state:
        try:
            t_global.con_pool = redis.ConnectionPool(host = t_global.args.roadblock_redis_server,
                                                     password = t_global.args.roadblock_redis_password,
                                                     port = 6379,
                                                     db = 0,
                                                     health_check_interval = 0)
            t_global.redcon = redis.Redis(connection_pool = t_global.con_pool)
            t_global.redcon.ping()
            t_global.con_pool_state = True
        except redis.exceptions.ConnectionError as con_error:
            print("%s" % (con_error))
            print("ERROR: Redis connection could not be opened!")
            time.sleep(3)

    t_global.pubsubcon = t_global.redcon.pubsub(ignore_subscribe_messages = True)

    t_global.con_watchdog_exit = threading.Event()
    t_global.con_watchdog = threading.Thread(target = connection_watchdog, args = ())
    t_global.con_watchdog.start()

    print("Roadblock UUID: %s" % (t_global.args.roadblock_uuid))
    print("Role: %s" % (t_global.args.roadblock_role))
    if t_global.args.roadblock_role == "follower":
        print("Follower ID: %s" % (t_global.args.roadblock_follower_id))
        print("Leader ID: %s" % (t_global.args.roadblock_leader_id))
    elif t_global.args.roadblock_role == "leader":
        print("Leader ID: %s" % (t_global.args.roadblock_leader_id))
        print("Followers: %s" % (t_global.args.roadblock_followers))
    if t_global.args.abort:
        print("Abort: True")
    else:
        print("Abort: False")

    # check if the roadblock was previously created and already timed
    # out -- ie. I am very late
    if key_check(t_global.args.roadblock_uuid + "__timedout"):
        do_timeout()

    # check if the roadblock has been initialized yet
    if key_set(t_global.args.roadblock_uuid, mytime):
        # i am creating the roadblock
        t_global.initiator = True
        print("Initiator: True")

        # set bus monitoring options
        t_global.watch_busA = False
        t_global.watch_busB = True
        t_global.mirror_busB = True

        # create busA
        list_append(t_global.args.roadblock_uuid + "__busA", message_to_str(message_build("all", "all", "initialized")))

        # create/subscribe to busB
        t_global.pubsubcon.subscribe(t_global.args.roadblock_uuid + "__busB")

        # publish the cluster timeout to busB
        print("Sending 'timeout-ts' message")
        message_publish(message_build("all", "all", "timeout-ts", cluster_timeout))

        # publish the initiator information to busB
        print("Sending 'initiator-info' message")
        message_publish(message_build("all", "all", "initiator-info"))
        t_global.initiator_type = t_global.args.roadblock_role
        t_global.initiator_id = t_global.my_id

        list_append(t_global.args.roadblock_uuid + "__initialized", int(True))
    else:
        print("Initiator: False")

        # the roadblock already exists, make sure it is initialized
        # completely before proceeding
        print("Waiting for roadblock initialization to complete")

        # wait until the initialized flag has been set for the roadblock
        while not key_check(t_global.args.roadblock_uuid + "__initialized"):
            time.sleep(1)
            print(".")

        print("Roadblock is initialized")

        # subscribe to busB
        t_global.pubsubcon.subscribe(t_global.args.roadblock_uuid + "__busB")

        # message myself on busB, once I receive this message on busA I will know I have processed all outstanding busA message and can move to monitoring busB
        if t_global.args.debug:
            print("Sending 'switch-buses' message")
        message_publish(message_build_custom(t_global.args.roadblock_role, "switch-buses", t_global.args.roadblock_role, t_global.my_id, "switch-buses"))

    if t_global.args.roadblock_role == "follower":
        # tell the leader that I am online
        print("Sending 'follower-online' message")
        message_publish(message_build("leader", t_global.args.roadblock_leader_id, "follower-online"))
    elif t_global.args.roadblock_role == "leader":
        # tell everyone that the leader is online
        print("Sending 'leader-online' message")
        message_publish(message_build("all", "all", "leader-online"))

    if t_global.initiator:
        # the initiator (first member to get to the roadblock) is
        # responsible for consuming messages from busB and copying
        # them onto busA so that they are preserved for other members
        # to receive once they arrive at the roadblock

        while t_global.mirror_busB:
            msg = t_global.pubsubcon.get_message()

            if not msg:
                time.sleep(0.001)
            else:
                msg_str = msg["data"].decode()
                if t_global.args.debug:
                    debug("initiator received msg=[%s] on busB" % (msg_str))

                msg = message_from_str(msg_str)

                if not message_validate(msg):
                    print("initiator received a message which did not validate! [%s]" % (msg_str))
                else:
                    # copy the message over to busA
                    if t_global.args.debug:
                        debug("initiator mirroring msg=[%s] to busA" % (msg_str))
                    list_append(t_global.args.roadblock_uuid + "__busA", msg_str)

                    if not message_for_me(msg):
                        if t_global.args.debug:
                            debug("initiator received a message which is not for me! [%s]" % (msg_str))
                    else:
                        if t_global.args.debug:
                            debug("initiator received a message for me! [%s]" % (msg_str))
                        ret_val = message_handle(msg)
                        if ret_val:
                            return(ret_val)

            if not t_global.mirror_busB:
                if t_global.args.debug:
                    debug("initiator stopping busB mirroring to busA")
    else:
        msg_list_index = -1
        get_out = False
        while t_global.watch_busA:
            # retrieve unprocessed messages from busA
            msg_list = list_fetch(t_global.args.roadblock_uuid + "__busA", msg_list_index+1)

            # process any retrieved messages
            if len(msg_list):
                for msg_str in msg_list:
                    msg_list_index += 1
                    if t_global.args.debug:
                        debug("received msg=[%s] on busA with status_index=[%d]" % (msg_str, msg_list_index))

                    msg = message_from_str(msg_str)

                    if not message_validate(msg):
                        print("received a message which did not validate! [%s]" % (msg_str))
                    else:
                        if not message_for_me(msg):
                            if t_global.args.debug:
                                debug("received a message which is not for me!")
                        else:
                            if t_global.args.debug:
                                debug("received a message which is for me!")
                            ret_val = message_handle(msg)
                            if ret_val:
                                return(ret_val)

            if t_global.watch_busA:
                time.sleep(1)

    if t_global.args.debug:
        debug("moving to common busB watch loop")

    while t_global.watch_busB:
        msg = t_global.pubsubcon.get_message()

        if not msg:
            time.sleep(0.001)
        else:
            msg_str = msg["data"].decode()
            if t_global.args.debug:
                debug("received msg=[%s] on busB" % (msg_str))

            msg = message_from_str(msg_str)

            if not message_validate(msg):
                print("received a message which did not validate! [%s]" % (msg_str))
            else:
                if not message_for_me(msg):
                    if t_global.args.debug:
                        debug("received a message which is not for me!")
                else:
                    if t_global.args.debug:
                        debug("received a message for me!")
                    ret_val = message_handle(msg)
                    if ret_val:
                        return(ret_val)

    print("Cleaning up")
    cleanup()

    print("Exiting")
    if t_global.leader_abort == True or t_global.follower_abort == True:
        print("Roadblock Completed with an Abort")
        return(-3)
    else:
        print("Roadblock Completed Successfully")
        return(0)

if __name__ == "__main__":
    exit(main())
