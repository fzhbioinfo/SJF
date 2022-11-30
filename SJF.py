# -*- coding:utf-8 -*-
from argparse import ArgumentParser
import pandas as pd
import subprocess
import logging
import time
import sys
import os
import re

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger.setLevel(logging.INFO)

ROOT = os.path.abspath(os.path.dirname(__file__))
CONFIG = os.path.join(ROOT, 'etc')
PIPELINE = os.path.join(ROOT, 'script')


class SubJobFrame:
    def __init__(self, flow, work_dir, info):
        # 类初始化,输入流程步骤的配置,分析的工作目录,样本信息
        # 流程步骤和样本信息转换成字典
        self.flow = pd.read_csv(flow, sep='\t')
        self.flow.index = self.flow['Name'].tolist()
        self.job_conf_dic = self.flow.to_dict('index')
        self.work_dir = work_dir
        self.info_df = pd.read_csv(info, sep='\t')
        self.info_df.index = self.info_df['sampleID'].tolist()
        self.info_dic = self.info_df.to_dict('index')

    def make(self):
        # 生成单样本步骤和批次步骤的分析脚本
        # 单样本步骤脚本在work_dir/sample/shell,批次步骤脚本在work_dir/shell
        # 第一步需要输入fq1,fq2
        for step in self.job_conf_dic.keys():
            pipe = os.path.join(PIPELINE, f'{step}.sh')
            if self.job_conf_dic[step]['Type'] == 'single':
                for sample in self.info_dic.keys():
                    script_dir = os.path.join(self.work_dir, sample, 'shell')
                    if not os.path.exists(script_dir):
                        os.makedirs(script_dir)
                    if self.job_conf_dic[step]['Parents'] != 'None':
                        with open(os.path.join(script_dir, f'{step}.sh'), 'w') as f:
                            content = '''#!/bin/bash
sh {} {} {} {}
'''.format(pipe, self.work_dir, ROOT, sample)
                            f.write(content)
                    else:
                        fq1 = self.info_dic[sample]['fq1']
                        fq2 = self.info_dic[sample]['fq2']
                        with open(os.path.join(script_dir, f'{step}.sh'), 'w') as f:
                            content = '''#!/bin/bash
sh {} {} {} {} {} {}
'''.format(pipe, self.work_dir, ROOT, sample, fq1, fq2)
                            f.write(content)
            else:
                script_dir = os.path.join(self.work_dir, 'shell')
                if not os.path.exists(script_dir):
                    os.makedirs(script_dir)
                with open(os.path.join(script_dir, f'{step}.sh'), 'w') as f:
                    content = '''#!/bin/bash
sh {} {} {}
'''.format(pipe, self.work_dir, ROOT)
                    f.write(content)

    @classmethod
    def create_job(cls, args):
        logger.info('Start Create Scripts!')
        cj = cls(args.step, args.work_dir, args.info)
        cj.make()
        logger.info('Create Scripts Finished!')

    @staticmethod
    def initialize_job(job_graph, job_conf_dic, job, step):
        # 任务相关信息初始化
        job_graph[job] = {}
        if os.path.exists(f'{job}.complete'):
            job_graph[job]['Status'] = 'complete'
        else:
            job_graph[job]['Status'] = 'incomplete'
        job_graph[job]['Name'] = step
        job_graph[job]['Type'] = job_conf_dic[step]['Type']
        job_graph[job]['Resources'] = job_conf_dic[step]['Resources']
        job_graph[job]['JobID'] = ''
        job_graph[job]['Children'] = []
        job_graph[job]['Parents'] = []
        return job_graph

    def generate_job_graph(self) -> dict:
        # 生成任务依赖关系字典
        job_graph = dict()
        job_batch_dir = os.path.join(self.work_dir, 'shell')
        for step in self.job_conf_dic.keys():
            if self.job_conf_dic[step]['Type'] == 'single':
                for sample in self.info_dic.keys():
                    job_single_dir = os.path.join(self.work_dir, sample, 'shell')
                    job = os.path.join(job_single_dir, f'{step}.sh')
                    job_graph = self.initialize_job(job_graph, self.job_conf_dic, job, step)
                    parents = self.job_conf_dic[step]['Parents'].split(',')
                    for parent in parents:
                        if parent != 'None':
                            if self.job_conf_dic[parent]['Type'] == 'single':
                                job_graph[job]['Parents'].append(os.path.join(job_single_dir, f'{parent}.sh'))
                            else:
                                job_graph[job]['Parents'].append(os.path.join(job_batch_dir, f'{parent}.sh'))
            else:
                job = os.path.join(job_batch_dir, f'{step}.sh')
                job_graph = self.initialize_job(job_graph, self.job_conf_dic, job, step)
                parents = self.job_conf_dic[step]['Parents'].split(',')
                for parent in parents:
                    if parent != 'None':
                        if self.job_conf_dic[parent]['Type'] == 'single':
                            for sample in self.info_dic.keys():
                                job_single_dir = os.path.join(self.work_dir, sample, 'shell')
                                job_graph[job]['Parents'].append(os.path.join(job_single_dir, f'{parent}.sh'))
                        else:
                            job_graph[job]['Parents'].append(os.path.join(job_batch_dir, f'{parent}.sh'))
        for job in job_graph.keys():
            for parent in job_graph[job]['Parents']:
                job_graph[parent]['Children'].append(job)
        return job_graph

    @staticmethod
    def job_num_in_sge():
        # SGE任务数目
        command = "qstat | grep `whoami` |wc -l"
        status, output = subprocess.getstatusoutput(command)
        return status, output

    @staticmethod
    def job_id_in_sge(command):
        # 获取投递任务的ID
        status, output = subprocess.getstatusoutput(command)
        try:
            job_id = re.findall(r"Your job (\d+) ", output)[0]
        except IndexError:
            job_id = ''
        return status, job_id

    @staticmethod
    def job_status_in_sge(job_id):
        # 检查任务是否还在运行或排队
        command = "qstat | grep " + "\"" + job_id + " " + "\""
        status, output = subprocess.getstatusoutput(command)
        return status, output

    @staticmethod
    def parents_status(job_graph, job):
        # 分析任务的所有父任务是否完成
        if len(job_graph[job]['Parents']) == 0:
            return 'complete'
        status_list = [job_graph[parent]['Status'] for parent in job_graph[job]['Parents']]
        if 'incomplete' not in status_list:
            return 'complete'
        else:
            return 'incomplete'

    @staticmethod
    def kill_job(job_graph, jobs):
        # 杀任务
        for job in jobs:
            if os.path.exists(f'{job}.complete'):
                continue
            if job_graph[job]['JobID'] != '':
                _ = subprocess.getoutput('qdel ' + job_graph[job]['JobID'])
        logger.info('Running and pending jobs were killed!')

    def submit(self, job_graph, job):
        # 投递任务
        status, job_num = self.job_num_in_sge()
        if status != 0:
            logger.info('qstat error!')
            sys.exit(1)
        while int(job_num) >= 4000:
            time.sleep(600)
            status, job_num = self.job_num_in_sge()
            if status != 0:
                logger.info('qstat error!')
                sys.exit(1)
        job_path = os.path.dirname(job)
        command = "qsub -wd " + job_path + " " + job_graph[job]['Resources'] + " " + job
        status, job_id = self.job_id_in_sge(command)
        return status, job_id

    @classmethod
    def work_flow(cls, args):
        # 生成流程图
        sjf = cls(args.step, args.work_dir, args.info)
        job_graph = sjf.generate_job_graph()
        logger.info('All Jobs Graph Created!')
        # 按流程图依赖顺序投递并监控任务状态
        jobs = list(filter(lambda x: len(job_graph[x]['Parents']) == 0, job_graph.keys()))
        while len(jobs) > 0:
            jobs_add = []
            jobs_remove = []
            for job in jobs:
                if os.path.exists(f'{job}.complete'):
                    job_graph[job]['Status'] = 'complete'
                if job_graph[job]['Status'] == 'incomplete':
                    if job_graph[job]['JobID'] == '':
                        if sjf.parents_status(job_graph, job) == 'complete':
                            status, job_id = sjf.submit(job_graph, job)
                            if status != 0:
                                logger.error(f'{job} Submit Failed!')
                                sys.exit(1)
                            job_graph[job]['JobID'] = job_id
                            logger.info(f'{job} Submit Success! JobID is {job_id}')
                    else:
                        status, output = sjf.job_status_in_sge(job_graph[job]['JobID'])
                        if status != 0 and output == '' and not os.path.exists(f'{job}.complete'):
                            time.sleep(60)
                            if not os.path.exists(f'{job}.complete'):
                                logger.error(f'{job} Run Failed!')
                                sjf.kill_job(job_graph, jobs)
                                sys.exit(1)
                else:
                    jobs_remove.append(job)
                    jobs_add += job_graph[job]['Children']
                    logger.info(f'{job} Finished!')
            if len(jobs_add) == 0:
                time.sleep(60)
            jobs = list(set(list(set(jobs) - set(jobs_remove)) + jobs_add))
        logger.info('All Jobs Finished!')


def main():
    parser = ArgumentParser()
    parser.add_argument('-step', help='pipeline all steps', default=os.path.join(CONFIG, 'allsteps.tsv'))
    parser.add_argument('-work_dir', help='work dir', required=True)
    parser.add_argument('-info', help='sample info', required=True)
    parser.add_argument('-create_only', help='only create scripts', action='store_true')
    parser.add_argument('-submit_only', help='only submit scripts', action='store_true')
    parsed_args = parser.parse_args()
    if not os.path.exists(parsed_args.work_dir):
        os.makedirs(parsed_args.work_dir)
    handler = logging.FileHandler(f'{parsed_args.work_dir}/auto.log')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    subprocess.Popen('cp ' + parsed_args.info + ' ' + os.path.join(parsed_args.work_dir, 'input.list'), shell=True)
    if not parsed_args.submit_only:
        SubJobFrame.create_job(parsed_args)
    if parsed_args.create_only:
        logger.info('Only Create Scripts and Exit!')
        sys.exit(0)
    SubJobFrame.work_flow(parsed_args)


if __name__ == '__main__':
    main()
