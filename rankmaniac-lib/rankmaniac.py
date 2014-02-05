"""
Simple wrapper for boto library to connect with AWS.

Written for the Rankmaniac competition (2013-2014)
in CS/EE 144: Ideas behind our Networked World
at the California Institute of Technology.

Authored by: Joe Wang (me@joewang.net)
Edited by: Max Hirschhorn (maxh@caltech.edu)
"""

import os
from time import localtime, strftime

# Amazon SDK for EC2
from boto.ec2.regioninfo import RegionInfo

# Amazon SDK for Elastic Map Reduce
from boto.emr.connection import EmrConnection
from boto.emr.step import StreamingStep

# Amazon SDK for S3
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class RankmaniacError(Exception):
    """General (catch-all) class for exceptions in this module."""
    pass

class Rankmaniac:
    """
    (wrapper class)

    This class presents a simple wrapper around the AWS SDK. It strives
    to provide all the functionality required to run map-reduce
    (Hadoop) on Amazon. This way the students do not need to worry about
    learning the API for Amazon S3 and EMR, and instead can focus on
    computing pagerank quickly!
    """

    DefaultRegionName = 'us-west-2'
    DefaultRegionEndpoint = 'elasticmapreduce.us-west-2.amazonaws.com'

    def __init__(self, team_id, access_key, secret_key,
                 bucket='cs144students'):
        """
        (constructor)

        Creates a new instance of the Rankmaniac class for a specific
        team using the provided credentials.

        Arguments:
            team_id       <str>     the team identifier, which may be
                                    differ slightly from the actual team
                                    name.

            access_key    <str>     the AWS access key identifier.
            secret_key    <str>     the AWS secret acess key.

        Keyword arguments:
            bucket        <str>     the S3 bucket name.
        """

        region = RegionInfo(None, self.DefaultRegionName,
                            self.DefaultRegionEndpoint)

        self._s3_bucket = bucket
        self._s3_conn = S3Connection(access_key, secret_key)
        self._emr_conn = EmrConnection(access_key, secret_key, region=region)

        self.team_id = team_id
        self.job_id = None

        self._reset()
        self._num_instances = 1

    def _reset(self):
        """
        Resets the internal state of the job and submission.
        """

        self._iter_no = 0
        self._infile = None
        self._last_outdir = None

        self._last_process_step_iter_no = -1
        self._is_done = False

    def __del__(self):
        """
        (destructor)

        Terminates the map-reduce job if any, and closes the connections
        to Amazon S3 and EMR.
        """

        if self.job_id is not None:
            self.terminate()

        self._s3_conn.close()
        self._emr_conn.close()

    def __enter__(self):
        """
        Used for `with` syntax. Simply returns this instance since the
        set-up has all been done in the constructor.
        """

        return self

    def __exit__(self, type, value, traceback):
        """
        Refer to __del__().
        """

        self.__del__()
        return False # do not swallow any exceptions

    def upload(self, indir='data'):
        """
        Uploads the local data to Amazon S3 under the configured bucket
        and key prefix (the team identifier). This way the code can be
        accessed by Amazon EMR to compute pagerank.

        Keyword arguments:
            indir       <str>       the base directory from which to
                                    upload contents.

        Special notes:
            This method only uploads **files** in the specified
            directory. It does not scan through subdirectories.

            WARNING! This method removes all previous (or ongoing)
            submission results, so it is unsafe to call while a job is
            already running (and possibly started elsewhere).
        """

        if self.job_id is not None:
            raise RankmaniacError('A job is already running.')

        bucket = self._s3_conn.get_bucket(self._s3_bucket)

        # Clear out current bucket contents for team
        keys = bucket.list(prefix=self._get_keyname())
        bucket.delete_keys(keys)

        for filename in os.listdir(indir):
            relpath = os.path.join(indir, filename)
            if os.path.isfile(relpath):
                keyname = self._get_keyname(filename)
                key = bucket.new_key(keyname)
                key.set_contents_from_filename(relpath)

    def set_infile(self, filename):
        """
        Sets the data file to use for the first iteration of the
        pagerank step in the map-reduce job.
        """

        if self.job_id is not None:
            raise RankmaniacError('A job is already running.')

        self._infile = filename

    def do_iter(self, pagerank_mapper, pagerank_reducer,
                process_mapper, process_reducer,
                pagerank_output=None, process_output=None,
                num_pagerank_mappers=1, num_pagerank_reducers=1):
        """
        Adds a pagerank step and a process step to the current job.
        """

        num_process_mappers = 1
        num_process_reducers = 1

        if self._iter_no == 0:
            pagerank_input = self._infile
        elif self._iter_no > 0:
            pagerank_input = self._last_outdir

        if pagerank_output is None:
            pagerank_output = self._get_default_outdir('pagerank')

        # Output from the pagerank step becomes input to process step
        process_input = pagerank_output

        if process_output is None:
            process_output = self._get_default_outdir('process')

        pagerank_step = self._make_step(pagerank_mapper, pagerank_reducer,
                                        pagerank_input, pagerank_output,
                                        num_pagerank_mappers,
                                        num_pagerank_reducers)

        process_step = self._make_step(process_mapper, process_reducer,
                                       process_input, process_output,
                                       num_process_mappers,
                                       num_process_reducers)

        steps = [pagerank_step, process_step]
        if self.job_id is None:
            self._submit_new_job(steps)
        else:
            self._emr_conn.add_jobflow_steps(self.job_id, steps)

        # Store `process_output` directory so it can be used in
        # subsequent iteration
        self._last_outdir = process_output
        self._iter_no += 1

    def is_done(self):
        """
        Returns `True` if the map-reduce job is done, and `False`
        otherwise.

        For all process-step output files that have not been fetched,
        gets the first part of the output file, and checks whether its
        contents begins with the string 'FinalRank'.

        Special notes:
            WARNING! The usage of this method in your code requires that
            that you used the default output directories in all calls
            to do_iter().
        """

        # Cache the result so we can return immediately without hitting
        # any of the Amazon APIs
        if self._is_done:
            return True

        iter_no = self._get_last_process_step_iter_no()
        if iter_no < 0:
            return False

        while self._last_process_step_iter_no < iter_no:
            self._last_process_step_iter_no += 1
            i = self._last_process_step_iter_no

            outdir = self._get_default_outdir('process', iter_no=i)
            keyname = self._get_keyname(outdir, 'part-00000')

            bucket = self._s3_conn.get_bucket(self._s3_bucket)
            key = Key(bucket=bucket, name=keyname)
            contents = key.next() # get first chunk of the output file

            if contents.startswith('FinalRank'):
                self._is_done = True # cache result
                break

        return self._is_done

    def is_alive(self):
        """
        Checks whether the jobflow has completed, failed, or been
        terminated.

        Special notes:
            WARNING! This method should only be called **after**
            is_done() in order to be able to distinguish between the
            cases where the map-reduce job has outputted 'FinalRank'
            on its final iteration and has a 'COMPLETED' state.
        """

        jobflow = self.describe()
        if jobflow.state in ('COMPLETED', 'FAILED', 'TERMINATED'):
            return False

        return True

    def terminate(self):
        """
        Terminates a running map-reduce job.
        """

        if not self.job_id:
            raise RankmaniacError('No job is running.')

        self._emr_conn.terminate_jobflow(self.job_id)
        self.job_id = None

        self._reset()

    def download(self, outdir='results'):
        """
        Downloads the results from Amazon S3 to the local directory.

        Keyword arguments:
            outdir      <str>       the base directory to which to
                                    download contents.

        Special notes:
            This method downloads all keys (files) from the configured
            bucket for this particular team. It creates subdirectories
            as needed.
        """

        bucket = self._s3_conn.get_bucket(self._s3_bucket)
        keys = bucket.list(prefix=self._get_keyname())
        for key in keys:
            keyname = key.name
            # Ignore folder keys
            if '$' not in keyname:
                suffix = keyname.split('/')[1:] # removes team identifier
                filename = os.path.join(outdir, *suffix)
                dirname = os.path.dirname(filename)

                if not os.path.exists(dirname):
                    os.makedirs(dirname)

                key.get_contents_to_filename(filename)

    def describe(self):
        """
        Gets the current map-reduce job details.

        Returns a boto.emr.emrobject.JobFlow object.

        Special notes:
            The JobFlow object has the following relevant fields.
                state       <str>           the state of the job flow,
                                            either COMPLETED
                                                 | FAILED
                                                 | TERMINATED
                                                 | RUNNING
                                                 | SHUTTING_DOWN
                                                 | STARTING
                                                 | WAITING

                steps       <list(boto.emr.emrobject.Step)>
                            a list of the step details in the workflow.

            The Step object has the following relevant fields.
                state               <str>       the state of the step.

                startdatetime       <str>       the start time of the
                                                job.

                enddatetime         <str>       the end time of the job.

            WARNING! Amazon has an upper-limit on the frequency with
            which you can call this method; we have had success with
            calling it at most once every 10 seconds.
        """

        if not self.job_id:
            raise RankmaniacError('No job is running.')

        return self._emr_conn.describe_jobflow(self.job_id)

    def _get_last_process_step_iter_no(self):
        """
        Returns the most recently process-step of the job flow that has
        been completed.
        """

        steps = self.describe().steps
        i = 1

        while i < len(steps):
            step = steps[i]
            if step.state != 'COMPLETED':
                break

            i += 2

        return i / 2 - 1

    def _get_default_outdir(self, name, iter_no=None):
        """
        Returns the default output directory, which is 'iter_no/name/'.
        """

        if iter_no is None:
            iter_no = self._iter_no

        # Return iter_no/name/ **with** the trailing slash
        return '%s/%s/' % (iter_no, name)

    def _submit_new_job(self, steps):
        """
        Submits a new job to run on Amazon EMR.
        """

        if self.job_id is not None:
            raise RankmaniacError('A job is already running.')

        job_name = self._make_name()
        num_instances = self._num_instances
        log_uri = self._get_s3_team_uri('job_logs')
        self.job_id = self._emr_conn.run_jobflow(name=job_name,
                                                 steps=steps,
                                                 num_instances=num_instances,
                                                 log_uri=log_uri)

    def _make_step(self, mapper, reducer, input, output,
                   num_mappers=1, num_reducers=1):
        """
        Returns a new step that runs the specified mapper and reducer,
        reading from the specified input and writing to the specified
        output.
        """

        bucket = self._s3_conn.get_bucket(self._s3_bucket)

        # Clear out current bucket/output contents for team
        keys = bucket.list(prefix=self._get_keyname(output))
        bucket.delete_keys(keys)

        step_name = self._make_name()
        step_args = ['-jobconf', 'mapred.map.tasks=%d' % (num_mappers),
                     '-jobconf', 'mapred.reduce.tasks=%d' % (num_reducers)]

        return StreamingStep(name=step_name,
                            step_args=step_args,
                            mapper=self._get_s3_team_uri(mapper),
                            reducer=self._get_s3_team_uri(reducer),
                            input=self._get_s3_team_uri(input),
                            output=self._get_s3_team_uri(output))

    def _make_name(self):
        return strftime('%%s %m-%d-%Y %H:%M:%S', localtime()) % (self.team_id)

    def _get_keyname(self, *args):
        """
        Returns the key name to use in the grading bucket (for the
        particular team).

            'team_id/...'
        """

        return '%s/%s' % (self.team_id, '/'.join(args))

    def _get_s3_team_uri(self, *args):
        """
        Returns the Amazon S3 URI for the team submissions.
        """

        return 's3n://%s/%s' % (self._s3_bucket, self._get_keyname(*args))
