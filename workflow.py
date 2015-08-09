#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
import logging
import time
from uuid import UUID
import random
from faker import Factory
#from pykafka import KafkaClient
import datetime

log = logging.getLogger()
log.setLevel('INFO')

class Config(object):
    cassandra_hosts = '127.0.0.1'
    #kafka_host = "127.0.0.1:9092"
    #kafka_topics = 'test'


def generate_coupon_data():
    results = []
    for i in range(90000,90100):
        for j in range(1,20):
            results.append([str(i),str(j),'coupon data', False, False, datetime.datetime.now()])
    return results

def generate_pd_data():
    results = []
    for i in range(1000,1100):
        for j in range(1,20):
            results.append([str(i),str(j),'coupon data', False, False,datetime.datetime.now()])
    return results

class SimpleClient(object):
    

    #Instantiate a session object to be used to connect to the database.
    session = None

    #Method to connect to the cluster and print connection info to the console
    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        log.info('Connected to cluster: ' + metadata.cluster_name)
        for host in metadata.all_hosts():
            log.info('Datacenter: %s; Host: %s; Rack: %s',
                host.datacenter, host.address, host.rack)

    #Close the connection
    def close(self):
        self.session.cluster.shutdown()
        log.info('Connection closed.')

    #Create the schema. This will drop the existing schema when the application is run.
    def create_schema(self):
        #self.session.execute("""DROP KEYSPACE IF EXISTS loyalty;""")
        self.session.execute("""CREATE KEYSPACE workflow WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};""")
        
        self.session.execute("""
            CREATE TABLE tasks_by_user (
              user_id text,
              account_id text,
              deal_id text,
              task_id text,
              description text,
              due_date timestamp,
              active boolean,
              priority text,
              PRIMARY KEY((user_id,account_id,deal_id),task_id)
            );

        """)

        self.session.execute("""
            CREATE TABLE users (
              user_id text PRIMARY KEY,
              user_name text,
              user_password text,
              valid boolean
            );
        """)

        self.session.execute("""
            CREATE TABLE accounts_by_user (
              user_id text,
              account_id text,
              PRIMARY KEY(user_id, account_id)
            );
        """)

        self.session.execute("""
            CREATE TABLE  deals_by_user (
              user_id text,
              account_id text,
              deal_id text,
              PRIMARY KEY((user_id),account_id,deal_id)
            );
        """)

        log.info('Workflow keyspace and schema created.')


class BoundStatementsClient(SimpleClient):
    def prepare_statements(self):
        self.create_user = self.session.prepare(
        """
            INSERT INTO workflow.users
            (user_id, user_email, user_name, user_password)
            VALUES (?,?,?,?);
        """)

        self.create_account = self.session.prepare(
        """
            INSERT INTO workflow.accounts_by_user
            (user_id, account_id)
            VALUES (?,?);
        """)

        self.create_deal = self.session.prepare(
        """
            INSERT INTO workflow.deals_by_user
            (user_id, account_id, deal_id)
            VALUES (?,?,?);
        """)

        self.create_task = self.session.prepare(
        """
            INSERT INTO workflow.tasks_by_user
            (user_id, account_id, deal_id, task_id, description, due_date, active_priority)
            VALUES (?,?,?,?,?,?,?);
        """)

        self.set_duedate = self.session.prepare(
        """
            INSERT INTO workflow.tasks_by_user
            (user_id, account_id, deal_id, task_id, due_date)
            VALUES (?,?,?,?,?);
        """)

        self.set_priority = self.session.prepare(
        """
            INSERT INTO workflow.tasks_by_user
            (user_id, account_id, deal_id, task_id, priority)
            VALUES (?,?,?,?,?);
        """)

        self.set_description = self.session.prepare(
        """
            INSERT INTO workflow.tasks_by_user
            (user_id, account_id, deal_id, task_id, description)
            VALUES (?,?,?,?,?);
        """)

        sef.set_active = self.session.prepare(
        """
            INSERT INTO workflow.tasks_by_user
            (user_id, account_id, deal_id, task_id, active)
            VALUES (?,?,?,?,?);
        """)

        self.get_deals = self.session.prepare(
        """
            SELECT user_id, account_id, deal_id FROM deals_by_user
            WHERE user_id = ?
        """)

        self.get_tasks = self.session.prepare(
        """
            SELECT user_id, account_id, deal_id FROM deals_by_user
            WHERE user_id = ?
        """)

        self.get_user = self.session.prepare(
        """
            SELECT user_id, user_name, user_password, valid FROM users
            WHERE user_id = ?
        """)



    def load_seed_data(self):
        coupon_data = generate_coupon_data()
        pd_data = generate_pd_data()
        #load coupon data
        for row in coupon_data:
            self.session.execute_async(self.insert_coupon,
                [row[0],row[1],row[2],row[3],row[4],row[5]]
            )
        #load pd data
        for row in pd_data:
            self.session.execute_async(self.insert_pd,
                [row[0],row[1],row[2],row[3],row[4],row[5]]
            )

    #load actual data like clips and likes of pds and coupons. in this example just households.
    def run_clips(self):
        #set up kafka producer
        kafka_client = KafkaClient(hosts=Config.kafka_host)
        kafka_topic = kafka_client.topics[kafka_topics]
        kafka_producer = kafka_topic.get_producer()
        for i in range(0,100000):
            for j in range(0,100):
                row_zip = str(random.randint(90000,90099))
                row_offer_id = str(random.randint(1000,1099))
                update_time = datetime.datetime.now()
                time_string = update_time.strftime("%Y-%m-%d %H:%M:%S")
                self.session.execute_async(self.insert_coupon_clip,
                    [row_zip,row_offer_id,True,update_time]
                )
                kafka_producer.produce([row_offer_id+','+time_string+','+str(1)])
            time.sleep(1)    


def main():
    logging.basicConfig()
    client = BoundStatementsClient()
    client.connect([Config.cassandra_hosts])
    client.create_schema()
    time.sleep(10)
    client.prepare_statements()
    client.load_seed_data()
    client.run_clips()
    client.close()

if __name__ == "__main__":
    main()

