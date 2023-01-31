# producer.py

import configparser

import sys
from imutils.object_detection import non_max_suppression

import imutils
import grequests
import json
import numpy as np
import skvideo.io
import time
import cv2
import datetime
from math import ceil
from kafka import SimpleProducer, SimpleClient
# from camerafeed import CameraFeed
from camerafeed.peopletracker import PeopleTracker
from camerafeed.tripline import Tripline

#  connect to Kafka
# camerafeed = CameraFeed()
# camerafeed.go_config(config_path='settings.ini')

class Producer:
    # frame dimension (calculated below in go)
    _frame_width = 0
    _frame_height = 0

    # how many frames processed
    _frame = 0

    kafka = SimpleClient('0.0.0.0:9092')
    producer = SimpleProducer(kafka)
    kafka_2 = SimpleClient('0.0.0.0:9093')
    producer2 = SimpleProducer(kafka_2)
    # Assign a topic
    topic = 'my-topic'

    def go_config(self, config_path='settings.ini'):
        # print("gere")
        # load config
        config = configparser.ConfigParser()
        config.read(config_path)
        self.font = cv2.FONT_HERSHEY_SIMPLEX
        self.endpoint = config.get('host', 'endpoint', fallback=None)

        # platform
        self.to_stdout = config.getboolean('platform', 'to_stdout')
        self.show_window = config.getboolean('platform', 'show_window')
        self.save_first_frame = config.getboolean('platform', 'save_first_frame')
        self.quit_after_first_frame = config.getboolean('platform', 'quit_after_first_frame')

        # video source settings
        self.crop_x1 = config.getint('video_source', 'frame_x1')
        self.crop_y1 = config.getint('video_source', 'frame_y1')
        self.crop_x2 = config.getint('video_source', 'frame_x2')
        self.crop_y2 = config.getint('video_source', 'frame_y2')
        self.max_width = config.getint('video_source', 'max_width')
        self.b_and_w = config.getboolean('video_source', 'b_and_w')

        # hog settings
        self.hog_win_stride = config.getint('hog', 'win_stride')
        self.hog_padding = config.getint('hog', 'padding')
        self.hog_scale = config.getfloat('hog', 'scale')

        # mog settings
        self.mog_enabled = config.getboolean('mog', 'enabled')
        if self.mog_enabled:
            self.mogbg = cv2.createBackgroundSubtractorMOG2()

        # setup lines
        lines = []
        total_lines = config.getint('triplines', 'total_lines')

        for idx in range(total_lines):
            key = 'line%d' % (idx + 1)
            start = eval(config.get('triplines', '%s_start' % key))
            end = eval(config.get('triplines', '%s_end' % key))
            buffer = config.getint('triplines', '%s_buffer' % key, fallback=10)
            direction_1 = config.get('triplines', '%s_direction_1' % key, fallback='Up')
            direction_2 = config.get('triplines', '%s_direction_2' % key, fallback='Down')
            line = Tripline(point_1=start, point_2=end, buffer_size=buffer, direction_1=direction_1,
                            direction_2=direction_2)
            lines.append(line)

        self.lines = lines
        self.source = config.get('video_source', 'source')
        self.people_options = dict(config.items('person'))

        # self.go()

    def go(self):

        # setup HUD
        self.last_time = time.time()

        # opencv 3.x bug??
        cv2.ocl.setUseOpenCL(False)

        # people tracking
        self.finder = PeopleTracker(people_options=self.people_options)

        # STARTS HERE
        # connect to camera
        self.camera = skvideo.io.vreader(self.source)

        # setup detectors
        self.hog = cv2.HOGDescriptor()
        self.hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())

        # feed in video
        print("emitting...")
        count = 0
        st_time = time.time()
        for frame in self.camera:
#        while self.camera.isOpened():
            #print(count)
            #count+=1
#            success, frame = self.camera.read()
            frame = self.process(frame)
            ed_time = time.time()
            if(ed_time-st_time>=10):
                st_time = ed_time
                a=Tripline.count
                Tripline.count=0
                count = ceil(a/4)
                st=datetime.datetime.fromtimestamp(ed_time).strftime('%Y-%m-%d %H:%M:%S')
                timestamp = str(st)+" "+str(count)+"\n"
                self.producer2.send_messages(self.topic, timestamp)
            # success, image = video.read()
            ret, jpeg = cv2.imencode('.png', frame)
            # Convert the image to bytes and send to kafka
            self.producer.send_messages(self.topic, jpeg.tobytes())

            # To reduce CPU usage create sleep time of 0.2sec
            # time.sleep(0.2)
            # cv2.imshow("After NMS", frame)
            # cv2.waitKey(0)
            # self.quit_after_first_frame or
            if self.quit_after_first_frame or cv2.waitKey(1) & 0xFF == ord('q'):
                print("stopped done")
                break
        print("done")

    def process(self, frame):

        if self.b_and_w:
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        # frame = self.crop_and_resize(frame)
        frame = imutils.resize(frame, width=400, height=600)
        print_frame_size = self._frame_height == 0

        self._frame_height = frame.shape[0]
        self._frame_width = frame.shape[1]

        if print_frame_size and not self.to_stdout:
            print('resized video to %dx%d' % (self._frame_width, self._frame_height))

        # frame = self.apply_mog(frame)
        frame = self.handle_the_people(frame)
        frame = self.render_hud(frame)

        if self.show_window:
            return frame
            # cv2.imshow('Camerafeed', frame)
            # cv2.waitKey(33)

        if self.to_stdout:
            sys.stdout.write(frame.tostring())
            # string = "".join(map(chr, frame.tostring()))
            # sys.stdout.write(string)

        if self.save_first_frame and self._frame == 0:
            cv2.imwrite('first_frame.png', frame)

    # help us crop/resize frames as they come in
    def crop_and_resize(self, frame):

        frame = frame[self.crop_y1:self.crop_y2, self.crop_x1:self.crop_x2]
        frame = imutils.resize(frame, width=min(self.max_width, frame.shape[1]))

        return frame

    # apply background subtraction if needed
    def apply_mog(self, frame):
        if self.mog_enabled:
            mask = self.mogbg.apply(frame)
            frame = cv2.bitwise_and(frame, frame, mask=mask)

        return frame

    # all the data that overlays the video
    def render_hud(self, frame):
        this_time = time.time()
        diff = this_time - self.last_time
        fps = 1 / diff
        message = 'FPS: %d' % fps
        # print(message)

        cv2.putText(frame, message, (10, self._frame_height - 20), self.font, 0.5, (255, 255, 255), 2)

        self.last_time = time.time()

        return frame

    def handle_the_people(self, frame):

        (rects, weight) = self.hog.detectMultiScale(frame, winStride=(self.hog_win_stride, self.hog_win_stride),
                                                    padding=(self.hog_padding, self.hog_padding), scale=self.hog_scale)
        # (rects, weight) = self.hog.detectMultiScale(frame, winStride=(4, 4), padding=(8, 8), scale=1.05)

        rects = np.array([[x, y, x + w, y + h] for (x, y, w, h) in rects])
        pick = non_max_suppression(rects, probs=None, overlapThresh=0.65)

        people = self.finder.people(pick)

        # draw triplines
        for line in self.lines:
            for person in people:
                if line.handle_collision(person) == 1:
                    self.new_collision(person)

            frame = line.draw(frame)

        for person in people:
            frame = person.draw(frame)
            person.colliding = False

        return frame

    def new_collision(self, person):

        post = {
            'name': person.name,
            'meta': json.dumps(person.meta),
            'date': time.time()
        }

        request = grequests.post(self.endpoint, data=post)
        grequests.map([request])


print("here1")
producer = Producer()
print("here2")
producer.go_config()
producer.go()
