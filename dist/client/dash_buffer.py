from __future__ import division
import Queue
import threading
import time
import csv
import os
import config_dash
from stop_watch import StopWatch
#modification by Arslan
#import pymongo
from pymongo import MongoClient
"""
modification tags the modification made by Arslan
Descrition of the database collections
qoe==> event based database update with the player stats
qoemetric==> only stalling events, stalling duration and time of occurance
qoefrequency==> frequecy based updates
"""
# Durations in seconds
PLAYER_STATES = ['INITIALIZED', 'INITIAL_BUFFERING', 'PLAY',
                 'PAUSE', 'BUFFERING', 'STOP', 'END']
EXIT_STATES = ['STOP', 'END']


class DashPlayer:
    """ DASH buffer class """
    """modification by Arslan--adding frequency (seconds) of monitoring probe"""
    frequency = 2.0
    def __init__(self, video_length, segment_duration):
        config_dash.LOG.info("Initializing the Buffer")
        #modification by Arslan for MongoClient IP of the mininet vm 192.168.56.103
        self.dbclient= MongoClient('192.168.56.103', 27017)
        # Read description above
        self.db=self.dbclient.qoemonitor.qoe
        self.dbqoemetric = self.dbclient.qoemonitor.qoemetric
        self.dbqoefrequency = self.dbclient.qoemonitor.qoefrequency
        """modification by Arslan to store action and bitrate as Variables"""
        self.Action_store = ""
        self.Bitrates_store = 0
        self.monitor = None
        """modification above"""
        self.player_thread = None
        self.playback_start_time = None
        self.playback_duration = video_length
        self.segment_duration = segment_duration
        #print "video_length = {}".format(video_length)
        #print "segment_duration = {}".format(segment_duration)
        # Timers to keep track of playback time and the actual time
        self.playback_timer = StopWatch()
        self.actual_start_time = None
        # Playback State
        self.playback_state = "INITIALIZED"
        self.playback_state_lock = threading.Lock()
        # Buffer size
        if config_dash.MAX_BUFFER_SIZE:
            self.max_buffer_size = config_dash.MAX_BUFFER_SIZE
        else:
            self.max_buffer_size = video_length
        # Duration of the current buffer
        self.buffer_length = 0
        self.buffer_length_lock = threading.Lock()
        # Buffer Constants
        self.initial_buffer = config_dash.INITIAL_BUFFERING_COUNT
        self.alpha = config_dash.ALPHA_BUFFER_COUNT
        self.beta = config_dash.BETA_BUFFER_COUNT
        self.segment_limit = None
        # Current video buffer that holds the segment data
        self.buffer = Queue.Queue()
        self.buffer_lock = threading.Lock()
        self.current_segment = None
        self.buffer_log_file = config_dash.BUFFER_LOG_FILENAME
        """modification by Arslan to collect qoe metrics """
        self.stalling= 0
        self.qoe_log_file = config_dash.QOE_LOG_FILENAME
        config_dash.LOG.info("VideoLength={},segmentDuration={},MaxBufferSize={},InitialBuffer(secs)={},"
                             "BufferAlph(secs)={},BufferBeta(secs)={}".format(self.playback_duration,
                                                                              self.segment_duration,
                                                                              self.max_buffer_size, self.initial_buffer,
                                                                              self.alpha, self.beta))

    def set_state(self, state):
        """ Function to set the state of the player"""
        state = state.upper()
        if state in PLAYER_STATES:
            self.playback_state_lock.acquire()
            config_dash.LOG.info("Changing state from {} to {} at {} Playback time ".format(self.playback_state, state,
                                                                                            self.playback_timer.time()))
            self.playback_state = state
            self.playback_state_lock.release()
        else:
            config_dash.LOG.error("Unidentified state: {}".format(state))

    def initialize_player(self):
        """Method that update the current playback time"""
        start_time = time.time()
        initial_wait = 0
        paused = False
        buffering = False
        interruption_start = None
        config_dash.LOG.info("Initialized player with video length {}".format(self.playback_duration))
        while True:
            # Video stopped by the user
            if self.playback_state == "END":
                config_dash.LOG.info("Finished playback of the video: {} seconds of video played for {} seconds".format(
                    self.playback_duration, time.time() - start_time))
                config_dash.JSON_HANDLE['playback_info']['end_time'] = time.time()
                self.playback_timer.pause()
                return "STOPPED"

            if self.playback_state == "STOP":
                # If video is stopped quit updating the playback time and exit player
                config_dash.LOG.info("Player Stopped at time {}".format(
                    time.time() - start_time))
                config_dash.JSON_HANDLE['playback_info']['end_time'] = time.time()
                self.playback_timer.pause()
                self.log_entry("Stopped")
                """modification by Arslan"""
                self.Action_store = "Stopped"
                return "STOPPED"

            # If paused by user
            if self.playback_state == "PAUSE":
                if not paused:
                    # do not update the playback time. Wait for the state to change
                    config_dash.LOG.info("Player Paused after {:4.2f} seconds of playback".format(
                        self.playback_timer.time()))
                    self.playback_timer.pause()
                    paused = True
                continue

            # If the playback encounters buffering during the playback
            if self.playback_state == "BUFFERING":
                if not buffering:
                    config_dash.LOG.info("Entering buffering stage after {} seconds of playback".format(
                        self.playback_timer.time()))
                    self.playback_timer.pause()
                    buffering = True
                    interruption_start = time.time()
                    config_dash.JSON_HANDLE['playback_info']['interruptions']['count'] += 1
                # If the size of the buffer is greater than the RE_BUFFERING_DURATION then start playback
                else:
                    # If the RE_BUFFERING_DURATION is greate than the remiang length of the video then do not wait
                    remaining_playback_time = self.playback_duration - self.playback_timer.time()
                    if ((self.buffer.qsize() >= config_dash.RE_BUFFERING_COUNT) or (
                            config_dash.RE_BUFFERING_COUNT * self.segment_duration >= remaining_playback_time
                            and self.buffer.qsize() > 0)):
                        buffering = False
                        if interruption_start:
                            interruption_end = time.time()
                            interruption = interruption_end - interruption_start
                            """modification by Arslan to log QoE metrics"""
                            self.qoe_log(stallingEvent=1, stalling_durations=interruption,
                                            stalling_start_time=interruption_start, stalling_end_time=interruption_end)
                            config_dash.JSON_HANDLE['playback_info']['interruptions']['events'].append(
                                (interruption_start, interruption_end))
                            config_dash.JSON_HANDLE['playback_info']['interruptions']['total_duration'] += interruption
                            config_dash.LOG.info("Duration of interruption = {}".format(interruption))
                            interruption_start = None
                        self.set_state("PLAY")
                        self.log_entry("Buffering-Play")
                        """modification by Arslan"""
                        self.Action_store = "Buffering-Play"

            if self.playback_state == "INITIAL_BUFFERING":
                if self.buffer.qsize() < config_dash.INITIAL_BUFFERING_COUNT:
                    initial_wait = time.time() - start_time
                    continue
                else:
                    config_dash.LOG.info("Initial Waiting Time = {}".format(initial_wait))
                    config_dash.JSON_HANDLE['playback_info']['initial_buffering_duration'] = initial_wait
                    config_dash.JSON_HANDLE['playback_info']['start_time'] = time.time()
                    self.set_state("PLAY")
                    self.log_entry("InitialBuffering-Play")
                    """modification by Arslan"""
                    self.Action_store = "InitialBuffering-Play"

            if self.playback_state == "PLAY":
                    # Check of the buffer has any segments
                    if self.playback_timer.time() == self.playback_duration:
                        self.set_state("END")
                        self.log_entry("Play-End")
                        """modification by Arslan"""
                        self.Action_store = "Play-End"
                    if self.buffer.qsize() == 0:
                        config_dash.LOG.info("Buffer empty after {} seconds of playback".format(
                            self.playback_timer.time()))
                        self.playback_timer.pause()
                        self.set_state("BUFFERING")
                        self.log_entry("Play-Buffering")
                        """modification by Arslan"""
                        self.Action_store = "Play-Buffering"
                        continue
                    # Read one the segment from the buffer
                    # Acquire Lock on the buffer and read a segment for it
                    self.buffer_lock.acquire()
                    play_segment = self.buffer.get()
                    self.buffer_lock.release()
                    config_dash.LOG.info("Reading the segment number {} from the buffer at playtime {}".format(
                        play_segment['segment_number'], self.playback_timer.time()))
                    self.log_entry(action="StillPlaying", bitrate=play_segment["bitrate"])
                    """modification by Arslan"""
                    self.Action_store = "StillPlaying"
                    self.Bitrates_store = play_segment["bitrate"]
                    # Calculate time playback when the segment finishes
                    future = self.playback_timer.time() + play_segment['playback_length']

                    # Start the playback
                    self.playback_timer.start()
                    while self.playback_timer.time() < future:
                        # If playback hasn't started yet, set the playback_start_time
                        if not self.playback_start_time:
                            self.playback_start_time = time.time()
                            config_dash.LOG.info("Started playing with representation {} at {}".format(
                                play_segment['bitrate'], self.playback_timer.time()))

                        # Duration for which the video was played in seconds (integer)
                        if self.playback_timer.time() >= self.playback_duration:
                            config_dash.LOG.info("Completed the video playback: {} seconds".format(
                                self.playback_duration))
                            self.playback_timer.pause()
                            self.set_state("END")
                            self.log_entry("TheEnd")
                            """modification by Arslan"""
                            self.Action_store = "TheEnd"
                            return
                    else:
                        self.buffer_length_lock.acquire()
                        self.buffer_length -= int(play_segment['playback_length'])
                        config_dash.LOG.debug("Decrementing buffer_length by {}. dash_buffer = {}".format(
                            play_segment['playback_length'], self.buffer_length))
                        self.buffer_length_lock.release()
                    if self.segment_limit:
                        if int(play_segment['segment_number']) >= self.segment_limit:
                            self.set_state("STOP")
                            config_dash.LOG.info("Stopped playback after segment {} at playtime {}".format(
                                play_segment['segment_number'], self.playback_duration))

    def write(self, segment):
        """ write segment to the buffer.
            Segment is dict with keys ['data', 'bitrate', 'playback_length', 'URI', 'size']
        """
        # Acquire Lock on the buffer and add a segment to it
        if not self.actual_start_time:
            self.actual_start_time = time.time()
            config_dash.JSON_HANDLE['playback_info']['start_time'] = self.actual_start_time
        config_dash.LOG.info("Writing segment {} at time {}".format(segment['segment_number'],
                                                                    time.time() - self.actual_start_time))
        self.buffer_lock.acquire()
        self.buffer.put(segment)
        self.buffer_lock.release()
        self.buffer_length_lock.acquire()
        self.buffer_length += int(segment['playback_length'])
        config_dash.LOG.debug("Incrementing buffer_length by {}. dash_buffer = {}".format(
            segment['playback_length'], self.buffer_length))
        self.buffer_length_lock.release()
        self.log_entry(action="Writing", bitrate=segment['bitrate'])
        """modification by Arslan"""
        self.Action_store = "Writing"
        self.Bitrates_store = segment['bitrate']
    """This is the method to update database with a given frequency"""
    def monitoring_probe(self):
        if self.actual_start_time:
            log_time = time.time() - self.actual_start_time
        else:
            log_time = 0
        update_monitor={"EpochTime": str(log_time), "CurrentPlaybackTime": str(self.playback_timer.time()),
                    "CurrentBufferSize": str(self.buffer.qsize()), "CurrentPlaybackState": str(self.playback_state),
                    "Action": str(self.Action_store), "Bitrate": str(self.Bitrates_store), "SysTime": str(time.time())}
        post_monitor=self.dbqoefrequency.insert_one(update_monitor).inserted_id
        self.monitor = threading.Timer(self.frequency,self.monitoring_probe)
        self.monitor.start()
        if self.Action_store=="TheEnd":
            self.monitor.cancel()
    def start(self):
        """ Start playback"""
        self.set_state("INITIAL_BUFFERING")
        self.log_entry("Starting")
        config_dash.LOG.info("Starting the Player")
        self.player_thread = threading.Thread(target=self.initialize_player)
        self.player_thread.daemon = True
        self.player_thread.start()
        self.log_entry(action="Starting")
        """"modification by Arslan to launch monitoring probe"""
        self.Action_store="Starting"
        self.monitoring_probe()
        #self.monitor = threading.Timer(2.0,self.monitoring_probe)
        #self.monitor.start()
    def stop(self):
        """Method to stop the playback"""
        self.set_state("STOP")
        self.log_entry("Stopped")
        config_dash.LOG.info("Stopped the playback")
        """modification by Arslan"""
        self.Action_store = "Stopped"
        #self.monitor.cancel()
    def log_entry(self, action, bitrate=0):
        """Method to log the current state"""
        if self.buffer_log_file:
            header_row = None
            if self.actual_start_time:
                log_time = time.time() - self.actual_start_time
            else:
                log_time = 0
            if not os.path.exists(self.buffer_log_file):
                header_row = "EpochTime,CurrentPlaybackTime,CurrentBufferSize,CurrentPlaybackState,Action,Bitrate".split(",")
                stats = (log_time, str(self.playback_timer.time()), self.buffer.qsize(),
                         self.playback_state, action,bitrate)
            else:
                stats = (log_time, str(self.playback_timer.time()), self.buffer.qsize(),
                         self.playback_state, action,bitrate)
            """modification by Arslan insert in db"""
            post_stat={"EpochTime": str(log_time), "CurrentPlaybackTime": str(self.playback_timer.time()),
                        "CurrentBufferSize": str(self.buffer.qsize()), "CurrentPlaybackState": str(self.playback_state),
                         "Action": str(action), "Bitrate": str(bitrate)}
            post=self.db.insert_one(post_stat).inserted_id
            str_stats = [str(i) for i in stats]
            with open(self.buffer_log_file, "ab") as log_file_handle:
                result_writer = csv.writer(log_file_handle, delimiter=",")
                if header_row:
                    result_writer.writerow(header_row)
                result_writer.writerow(str_stats)
            config_dash.LOG.info("BufferStats: EpochTime=%s,CurrentPlaybackTime=%s,CurrentBufferSize=%s,"
                                 "CurrentPlaybackState=%s,Action=%s,Bitrate=%s" % tuple(str_stats))

        """modification by Arslan to log QoE metrics"""
        def qoe_log(self, stallingEvent=0, stalling_durations=0, stalling_start_time=0, stalling_end_time=0):
            """method to log QoE metrics and save it to database"""
            """TODO add database connection to QoE Metric"""
            """for frequency based approach make a function to send data in
            database using above log file and call it in the start of the start"""
            self.stalling = self.stalling + stallingEvent
            if self.qoe_log_file:
                header_row = None
                if self.actual_start_time:
                    log_time = time.time() - self.actual_start_time
                else:
                    log_time = 0
                if not os.path.exists(self.qoe_log_file):
                    header_row = "EpochTime,CurrentPlaybackTime,StallingEvent,StallingDurations,StallingStart,StallingEnd".split(",")
                    stats = (log_time, str(self.playback_timer.time()), self.stalling,
                             stalling_durations, stalling_start_time,stalling_end_time)
                else:
                    stats = (log_time, str(self.playback_timer.time()), self.stalling,
                             stalling_durations, stalling_start_time,stalling_end_time)
            db_entry = {"EpochTime": str(log_time), "CurrentPlaybackTime": str(self.playback_timer.time()),
                        "StallingEvent": str(self.stalling), "StallingDurations": str (stalling_durations),
                         "StallingStart":str(stalling_start_time), "StallingEnd": str(stalling_end_time)}
            post_db= self.dbqoemetric.insert_one(db_entry).inserted_id
            with open(self.qoe_log_file, "ab") as log_file_handle:
                result_writer = csv.writer(log_file_handle, delimiter=",")
                if header_row:
                    result_writer.writerow(header_row)
                result_writer.writerow(str_stats)
