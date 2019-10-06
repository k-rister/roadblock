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
    redcon.delete(t_global.args.roadblock_uuid + '__status')
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
    if t_global.args.roadblock_role == 'leader' and redcon.msetnx({t_global.args.roadblock_uuid: mytime}):
    #if redcon.msetnx({t_global.args.roadblock_uuid: mytime}):
        # i am creating the roadblock
        print("first!")
        redcon.msetnx({t_global.args.roadblock_uuid + '__timeout': timeout})
        redcon.rpush(t_global.args.roadblock_uuid + '__status', 'initialized')
    else:
        # the roadblock already exists, make sure it is initialized
        # completely before proceeding
        print("not first...")
        while not redcon.exists(t_global.args.roadblock_uuid + '__status'):
            time.sleep(1)
            print(".")

        timeout = redcon.get(t_global.args.roadblock_uuid + '__timeout')

    print("Timeout: %s" % (datetime.datetime.utcfromtimestamp(int(timeout)).strftime("%H:%M:%S on %Y-%m-%d")))

    stage = 0
    status_index = -1
    followers_ready = 0
    while True:
        status_list = redcon.lrange(t_global.args.roadblock_uuid + '__status', status_index+1, -1)
        for msg in status_list:
            status_index += 1
            msg = msg.decode()
            print("received msg=[%s] status_index=[%d]" % (msg, status_index))
            
            if msg == 'initialized':
                state = 1
            elif msg == 'leader_ready':
                state = 2
            elif msg == 'followers_ready':
                followers_ready += 1

        if t_global.args.roadblock_role == 'leader' and state == 2 and followers_ready == len(t_global.args.roadblock_followers):
            state = 3
                
        if state == 1:
            if t_global.args.roadblock_role == 'leader':
                pubsubcon.psubscribe(t_global.args.roadblock_uuid + '__followers__*')
                redcon.rpush(t_global.args.roadblock_uuid + '__status', 'leader_ready')
        elif state == 2:
            if t_global.args.roadblock_role == 'follower':
                print("Signaling ready")
                redcon.publish(t_global.args.roadblock_uuid + '__followers__' + t_global.args.roadblock_follower_id, 'ready')
                redcon.rpush(t_global.args.roadblock_uuid + '__status', 'followers_ready')
                pubsubcon.subscribe(t_global.args.roadblock_uuid + '__leader')
                state = 3
        elif state == 3:
            if t_global.args.roadblock_role == 'leader':
                for msg in pubsubcon.listen():
                    #print(msg)
                    channel = msg['channel'].decode().split("__")
                    if msg['data'].decode() == 'ready':
                        print("Received ready from %s" % (channel[2]))
                        del followers['ready'][channel[2]]

                    if len(followers['ready']) == 0:
                        print("All followers ready")
                        print("Signaling go")
                        redcon.publish(t_global.args.roadblock_uuid + '__leader', "go")
                        state = 4
                        break
            elif t_global.args.roadblock_role == 'follower':
                for msg in pubsubcon.listen():
                    #print(msg)
                    if msg['data'].decode() == 'go':
                        print("Received go from leader")
                        print("Signaling gone")
                        redcon.publish(t_global.args.roadblock_uuid + '__followers__' + t_global.args.roadblock_follower_id, 'gone')
                        print("Exiting")
                        return(0)
        elif state == 4:
            if t_global.args.roadblock_role == 'leader':
                for msg in pubsubcon.listen():
                    #print(msg)
                    channel = msg['channel'].decode().split("__")
                    if msg['data'].decode() == 'gone':
                        print("Received gone from %s" % (channel[2]))
                        del followers['gone'][channel[2]]

                    if len(followers['gone']) == 0:
                        print("All followers gone")
                        print("Cleaning up")
                        cleanup(redcon)
                        break

                print("Exiting")
                return(0)

        time.sleep(1)

if __name__ == "__main__":
    exit(main())
