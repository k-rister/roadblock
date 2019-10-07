#!/usr/bin/python3

import sys, getopt
import argparse
import json
import string
import datetime
import math
import time
import calendar
import socket
import redis

class t_global(object):
    args=None;

def process_options ():
    parser = argparse.ArgumentParser(usage="roadblock testing");

    parser.add_argument('--uuid',
                        dest = 'roadblock_uuid',
                        help = 'UUID that maps to the specific roadblock being processed.',
                        required = True)

    parser.add_argument('--role',
                        dest = 'roadblock_role',
                        help = 'What is the roadblock role of this node.',
                        default = 'follower',
                        choices = ['leader', 'follower'])

    parser.add_argument('--timeout',
                        dest = 'roadblock_timeout',
                        help = 'How long should the roadblock wait before timing out.',
                        default = 30,
                        type = int)

    parser.add_argument('--follower-id',
                        dest = 'roadblock_follower_id',
                        help = 'What is follower ID for this node.',
                        default = socket.getfqdn(),
                        type = str)

    parser.add_argument('--followers',
                        dest = 'roadblock_followers',
                        help = 'Use one or more times on the leader to specify the followers by name.',
                        action = 'append',
                        type = str)

    t_global.args = parser.parse_args();

def cleanup(redcon):
    redcon.delete(t_global.args.roadblock_uuid)
    redcon.delete(t_global.args.roadblock_uuid + '__timeout')
    redcon.delete(t_global.args.roadblock_uuid + '__initialized')
    redcon.delete(t_global.args.roadblock_uuid + '__online-status')
    return(0)

def main():
    process_options()

    followers = { 'ready': {},
                  'gone': {} }
    if t_global.args.roadblock_role == 'leader':
        if len(t_global.args.roadblock_followers) == 0:
            print("ERROR: There must be at least one follower")
            return(-1)
        
        for follower in t_global.args.roadblock_followers:
            followers['ready'][follower] = True
            followers['gone'][follower] = True
    
    redcon = redis.Redis(host = '10.88.0.10',
                         port = 6379,
                         password = '')
    pubsubcon = redcon.pubsub(ignore_subscribe_messages = True)

    print("Role: %s" % (t_global.args.roadblock_role))
    if t_global.args.roadblock_role == 'follower':
        print("Follower ID: %s" % (t_global.args.roadblock_follower_id))
    elif t_global.args.roadblock_role == 'leader':
        print("Followers: %s" % (t_global.args.roadblock_followers))
    
    mytime = calendar.timegm(time.gmtime())
    timeout = mytime + t_global.args.roadblock_timeout
    if redcon.msetnx({t_global.args.roadblock_uuid: mytime}):
        # i am creating the roadblock
        print("Initializing roadblock as first arriving member")
        redcon.msetnx({t_global.args.roadblock_uuid + '__timeout': timeout})
        redcon.rpush(t_global.args.roadblock_uuid + '__online-status', 'initialized')
        redcon.rpush(t_global.args.roadblock_uuid + '__initialized', int(True))
    else:
        # the roadblock already exists, make sure it is initialized
        # completely before proceeding
        print("I am not the first arriving member, waiting for roadblock initialization to complete")

        # wait until the initialized flag has been set for the roadblock
        while not redcon.exists(t_global.args.roadblock_uuid + '__initialized'):
            time.sleep(1)
            print(".")

        print("Roadblock is initialized")

        # retrieve the posted timeout so that the same timestamp is
        # shared across all members of the roadblock
        timeout = redcon.get(t_global.args.roadblock_uuid + '__timeout')

    print("Timeout: %s" % (datetime.datetime.utcfromtimestamp(int(timeout)).strftime("%H:%M:%S on %Y-%m-%d")))

    state = 0
    status_index = -1
    while True:
        # retrieve unprocessed status messages
        status_list = redcon.lrange(t_global.args.roadblock_uuid + '__online-status', status_index+1, -1)

        # process any retrieved status messages
        if len(status_list):
            for msg in status_list:
                status_index += 1
                msg = msg.decode().split('/')
                #print("received msg=[%s] status_index=[%d]" % (msg, status_index))
                
                if msg[0] == 'initialized':
                    state = 1
                elif msg[0] == 'leader_online':
                    if t_global.args.roadblock_role == 'follower':
                        print("Received online status from leader")
                    state = 2

        if state == 1:
            if t_global.args.roadblock_role == 'leader':
                # listen for messages published from the followers
                pubsubcon.subscribe(t_global.args.roadblock_uuid + '__followers')

                print("Signaling online")
                redcon.rpush(t_global.args.roadblock_uuid + '__online-status', 'leader_online')

                break
        elif state == 2:
            if t_global.args.roadblock_role == 'follower':
                # listen for messages published from the leader
                pubsubcon.subscribe(t_global.args.roadblock_uuid + '__leader')

                print("Publishing ready message")
                redcon.publish(t_global.args.roadblock_uuid + '__followers', t_global.args.roadblock_follower_id + '/ready')
                break

        time.sleep(1)

    if t_global.args.roadblock_role == 'leader':
        for msg in pubsubcon.listen():
            #print(msg)
            msg = msg['data'].decode().split('/')
            if msg[1] == 'ready':
                if msg[0] in followers['ready']:
                    print("Received ready message from '%s'" % (msg[0]))
                    del followers['ready'][msg[0]]
                elif msg[0] in t_global.args.roadblock_followers:
                    print("Did I already process this ready message from follower '%s'?" % (msg[0]))
                else:
                    print("Received ready message from unknown follower '%s'" % (msg[0]))

            if len(followers['ready']) == 0:
                print("All followers ready")
                print("Publishing go message")
                redcon.publish(t_global.args.roadblock_uuid + '__leader', "go")
                break
    elif t_global.args.roadblock_role == 'follower':
        for msg in pubsubcon.listen():
            #print(msg)
            if msg['data'].decode() == 'go':
                print("Received go message from leader")
                print("Publishing gone message")
                redcon.publish(t_global.args.roadblock_uuid + '__followers', t_global.args.roadblock_follower_id + '/gone')
                print("Exiting")
                return(0)

    if t_global.args.roadblock_role == 'leader':
        for msg in pubsubcon.listen():
            #print(msg)
            msg = msg['data'].decode().split('/')
            if msg[1] == 'gone':
                if msg[0] in followers['gone']:
                    print("Received gone message from '%s'" % (msg[0]))
                    del followers['gone'][msg[0]]
                elif msg[0] in t_global.args.roadblock_followers:
                    print("Did I already process this gone message from follower '%s'?" % (msg[0]))
                else:
                    print("Received gone message from unknown follower '%s'" % (msg[0]))

            if len(followers['gone']) == 0:
                print("All followers gone")
                print("Cleaning up")
                cleanup(redcon)
                break

        print("Exiting")
        return(0)

if __name__ == "__main__":
    exit(main())
