# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from scrapy.selector import Selector

import csv
import pickle
import os.path


def save_obj(obj, name):
    with open(name, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_csv(name):
    with open(name, 'rb') as f:
        reader = csv.reader(f, delimiter='\n')
        csv_list = list(reader)
    flat_list = [item.lower() for sublist in csv_list for item in sublist]
    return flat_list


def load_obj(name):
    with open(name, 'rb') as f:
        return pickle.load(f)


class CollegeCrawler(scrapy.Spider):
    name = 'college-crawler'

    csv_college_filename = './data/us_college.csv'
    csv_college_file = open(csv_college_filename, 'a')
    csv_college_writer = csv.DictWriter(csv_college_file, fieldnames=['name', 'state'])

    college_set = set()
    set_name = './data/college-set.pkl'

    def start_requests(self):
        if os.path.isfile(self.set_name):
            self.college_set = load_obj(self.set_name)
        else:
            self.csv_college_writer.writeheader()

        base_url = 'http://www.collegesimply.com/colleges/'
        csv_state_filename = './data/us_state_names.csv'
        state_list = load_csv(csv_state_filename)
        # state_list = ['alaska']
        for state in state_list:
            yield Request(url=base_url+state, callback=self.parse, meta={'state': state})

    def parse(self, response):
        state = response.meta['state']
        selector = Selector(text=response.body)
        base_xpath = '//table[contains(@class, "table table-striped table-hover table-responsive")]/tbody/tr'
        college_selector_list = selector.xpath(base_xpath)
        for idx in xrange(1, 1+len(college_selector_list)):
            college_name = str(college_selector_list.xpath(base_xpath+"["+str(idx)+']/td/a/text()').extract_first())
            if (college_name != 'None') and (college_name not in self.college_set):
                self.college_set.add(college_name)
                self.save(college_name, state)

    def save(self, name, state):
        self.csv_college_writer.writerow({'name': name, 'state': state})

    def close(self, reason):
        self.csv_college_file.close()
        save_obj(self.college_set, self.set_name)
        print "len: ", len(self.college_set)
        print "-------- end parse --------"
