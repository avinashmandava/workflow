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

class User(object):
    def __init__(self,user_id,user_name,user_password,valid):
        self.user_id = user_id
        self.user_name = user_name
        self.user_password = user_password
        self.valid = valid

class Account(object):
    def __init__(self,user_id,account_id):
        self.user_id = user_id
        self.account_id = account_id

class Deal(object):
    def __init__(self,user_id,account_id):
        self.user_id = user_id
        self.account_id = account_id
        self.deal_id = deal_id

class Task(object):
    def __init__(self,user_id,account_id,deal_id,task_id,description,due_date,active,priority):
        self.user_id = user_id
        self.account_id = account_id
        self.deal_id = deal_id
        self.task_id = task_id
        self.description = description
        self.due_date = due_date
        self.active = active
        self.priority = priority

class Config(object):
    cassandra_hosts = '127.0.0.1'
    #kafka_host = "127.0.0.1:9092"
    #kafka_topics = 'test'

class Generator(object):
    def generate_users(self,user_count):
        fake = Factory.create()
        users = []
        for i in range(1,user_count):
            user = User(fake.email(),fake.name(),'welcome',True)
            users.append(user])
        return users

    def generate_accounts(self,users,account_count):
        fake = Factory.create()
        accounts = []
        for user in users:
                for i in range(1,account_count):
                    account = Account(user.user_id,fake.company())
                    accounts.append(account)
        return accounts

    def generate_deals(self,accounts,deal_count):
        fake = Factory.create()
        deals = []
        for account in accounts:
            for i in range(1,deal_count):
                deal = Deal(account.user_id,account.account_id,str(i))
                deals.append(Deal)
        return deals

    def generate_tasks(self,deals,task_count):
        fake = Factory.create()
        tasks = []
        for deal in deals:
            for i in rance(1,task_count):
                task = Task(deal.user_id,deal.account_id,deal.deal_id,fake.bs(),fake.catch_phrase(),fake.date_time_between(start_date="now", end_date="+1y"),True,fake.random_element(['H', 'M', 'L']))
                tasks.append(task)
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
        self.session.execute("""DROP KEYSPACE IF EXISTS workflow;""")
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
            (user_id, user_name, user_password, valid)
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
            VALUES (?,?,?,?,?,?,?,?);
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

        self.set_active = self.session.prepare(
        """
            INSERT INTO workflow.tasks_by_user
            (user_id, account_id, deal_id, task_id, active)
            VALUES (?,?,?,?,?);
        """)

        self.get_deals = self.session.prepare(
        """
            SELECT user_id, account_id, deal_id FROM workflow.deals_by_user
            WHERE user_id = ? AND account_id = ?
        """)

        self.get_tasks = self.session.prepare(
        """
            SELECT user_id, account_id, deal_id FROM workflow.tasks_by_user
            WHERE user_id = ? AND account_id = ? AND deal_id = ?
        """)

        self.get_user = self.session.prepare(
        """
            SELECT user_id, user_name, user_password, valid FROM workflow.users
            WHERE user_id = ?
        """)



    def load_seed_data(self):
        users = Generator().generate_users(100)
        accounts = Generator().generate_accounts(users,10)
        deals = Generator().generate_deals(accounts,5)
        tasks = Generator().generate_tasks(deals,10)
        #load coupon data
        for user in users:
            self.session.execute_async(self.create_user,
                [user.user_id,user.user_name,user.user_password,user.valid]
            )

        for account in accounts:
            self.session.execute_async(self.create_account,
                [account.user_id,account.account_id]
            )

        for deal in deals:
            self.session.execute_async(self.create_deal,
                [deal.user_id,deal.account_id,deal.deal_id]
            )

        for task in tasks:
            self.session.execute_async(self.create_task,
                [task.user_id,task.account_id,task.deal_id,task.task_id,task.description,task.due_date,task.active,task.priority]
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
