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

class Generator(object):
    def generate_users(user_count):
        fake = Factory.create()
        users = []
        for i in range(1,user_count):
            user_id = fake.email()
            user_name = fake.name()
            user_password = 'welcome'
            active = True
            users.append([user_id, user_name, user_password, active])
        return users

    def generate_accounts(users,account_count):
        fake = Factory.create()
        accounts = []
        for user in users:
                user_id = user[0]
                for i in range(1,account_count):
                    account_id = fake.company()
                    accounts.append([user_id,account_id])
        return accounts

    def generate_deals(accounts,deal_count):
        fake = Factory.create()
        deals = []
        for account in accounts:
            user_id = account[0]
            account_id = account[1]
            for i in range(1,deal_count):
                deal_id = str(i)
                deals.append([user_id,account_id,deal_id])
        return deals

    def generate_tasks(deals,task_count):
        fake = Factory.create()
        tasks = []
        for deal in deals:
            user_id = deal[0]
            account_id = deal[1]
            deal_id = deal[2]
            task_id = fake.bs()
            description = fake.catch_phrase()
            due_date = fake.date_time_between(start_date="now", end_date="+1y")
            active = True
            priority = fake.random_element(array=('H', 'M', 'L'))
            tasks.append([user_id,account_id,deal_id,task_id,description,due_date,active,priority])
        return tasks

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
            CREATE TABLE workflow.tasks_by_user (
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
            CREATE TABLE workflow.users (
              user_id text PRIMARY KEY,
              user_name text,
              user_password text,
              valid boolean
            );
        """)

        self.session.execute("""
            CREATE TABLE workflow.accounts_by_user (
              user_id text,
              account_id text,
              PRIMARY KEY(user_id, account_id)
            );
        """)

        self.session.execute("""
            CREATE TABLE workflow.deals_by_user (
              user_id text,
              account_id text,
              deal_id text,
              PRIMARY KEY((user_id),account_id,deal_id)
            );
        """)

        log.info('Workflow keyspace and schema created.')


class WorkflowClient(SimpleClient):
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
            (user_id, account_id, deal_id, task_id, description, due_date, active, priority)
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
        users = Generator.generate_users(100)
        accounts = Generator.generate_accounts(users,10)
        deals = Generator.generate_deals(accounts,5)
        tasks = Generator.generate_tasks(deals,10)
        #load coupon data
        for row in users:
            self.session.execute_async(self.create_user,
                [row[0],row[1],row[2],row[3]]
            )

        for row in accounts:
            self.session.execute_async(self.create_account,
                [row[0],row[1]]
            )

        for row in deals:
            self.session.execute_async(self.create_deal,
                [row[0],row[1],row[2]]
            )

        for row in tasks:
            self.session.execute_async(self.create_task,
                [row[0],row[1],row[2],row[3],row[4],row[5],row[6]]
            )


def main():
    logging.basicConfig()
    client = WorkflowClient()
    client.connect([Config.cassandra_hosts])
    client.create_schema()
    time.sleep(10)
    client.prepare_statements()
    client.load_seed_data()
    client.close()

if __name__ == "__main__":
    main()
