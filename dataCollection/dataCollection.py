from datasift import Client
import logging
import os
import time
import json
import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
#handler = logging.StreamHandler()
handler = logging.FileHandler('SoSweet.log')
handler.setFormatter(logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"))
logger.addHandler(handler)


ds_username = 'jpmague'
ds_api_key = 'cc10a025cf50bedb1cc51816cf25a007'
csdl = '''(twitter.lang in "fr" OR twitter.user.lang in "fr")
AND
twitter.user.time_zone contains_any "Casablanca,Dublin,Edinburgh,Lisbon,London,Monrovia,Amsterdam,Belgrade,Berlin,Bern,Bratislava,Brussels,Budapest,Copenhagen,Ljubljana,Madrid,Paris,Prague,Rome,Sarajevo,Skopje,Stockholm,Vienna,Warsaw,West Central Africa,Zagreb"
AND
interaction.sample < 10'''


#data_path = "/Users/jmague/Documents/Projets/SoSweet/data"
data_path ="/datastore/complexnet/twitter/data"


class DataCollector(object):
    def __init__(self, ds_username, ds_api_key, csdl, data_path):
        try:
            logger.debug("Connecting to Datasift")
            self.ds_client = Client(ds_username, ds_api_key)
            self.data_path = data_path
            self.current_file_for_received_tweets = None
            logger.info("Connected to Datasift as %s" % ds_username)
            logger.debug("Compiling query")
            self.stream = self.ds_client.compile(csdl)['hash']
            logger.debug("query compiled")
        except Exception as e:
            logger.exception("exception in DataCollector.__init__%s" % str(e))
            self.current_file_for_received_tweets.close()
            logger.debug("closing %s" % self.current_file_for_received_tweets.name)
            raise e

        @self.ds_client.on_delete
        def on_delete(msg):
            try:
                logger.debug("deleted tweet")
                f = open("%s/%s.deleted"%(self.data_path, datetime.datetime.utcnow().strftime("%Y-%m-%dT%H")),'a')
                logger.debug("Opening %s" % f.name)
                f.write(json.dumps(msg, default = json_date_handler)+'\n')
                f.close()
            except Exception as e:
                logger.exception("exception in on_delete: %s" % str(e))
                self.current_file_for_received_tweets.close()
                logger.debug("closing %s" % self.current_file_for_received_tweets.name)
                pid = os.getpid()
                logger.critical("Killing collecting process: %d" % pid)
                os.kill(pid, 1)

        @self.ds_client.on_open
        def on_open():
            try:
                logger.info("Opening stream")
                logger.debug('Waiting for tweets')
            except Exception as e:
                logger.exception("exception in on_open: %s" % str(e))
                self.current_file_for_received_tweets.close()
                logger.debug("closing %s" % self.current_file_for_received_tweets.name)
                pid = os.getpid()
                logger.critical("Killing collecting process: %d" % pid)
                os.kill(pid, 1)

            @self.ds_client.subscribe(self.stream)
            def on_interaction(msg):
                try:
                    # logger.debug("tweet received")
                    creation_date = msg['twitter']['created_at']
                    if self.current_file_for_received_tweets is None or self.current_file_for_received_tweets.closed:
                        self.current_file_for_received_tweets=open("%s/%s.data"%(self.data_path,creation_date.strftime("%Y-%m-%dT%H")),'a')
                        logger.debug("opening %s" % self.current_file_for_received_tweets.name)
                    if self.current_file_for_received_tweets.name != "%s/%s.data"%(self.data_path,creation_date.strftime("%Y-%m-%dT%H")):
                        logger.debug("File switching")
                        logger.debug("closing %s" % self.current_file_for_received_tweets.name)
                        self.current_file_for_received_tweets.close()
                        self.current_file_for_received_tweets=open("%s/%s.data"%(self.data_path,creation_date.strftime("%Y-%m-%dT%H")),'a')
                        logger.debug("opening %s" % self.current_file_for_received_tweets.name)
                    self.current_file_for_received_tweets.write(json.dumps(msg, default = json_date_handler)+'\n')
#                    self.current_file_for_received_tweets.close()
                except Exception as e:
                    logger.exception("exception in on_interaction: %s" % str(e))
                    logger.debug("closing %s" % self.current_file_for_received_tweets.name)
                    self.current_file_for_received_tweets.close()
                    pid = os.getpid()
                    logger.critical("Killing collecting process: %d" % pid)
                    os.kill(pid, 1)
                    #self.stopCollection()
                    #raise e

        @self.ds_client.on_closed
        def on_close(wasClean, code, reason):
            try:
                logger.info('Closing stream : %s' % reason)
                self.current_file_for_received_tweets.close()
                logger.debug("closing %s" % self.current_file_for_received_tweets.name)
            except Exception as e:
                logger.exception("exception in onClose: %s" % str(e))
                logger.debug("closing %s" % self.current_file_for_received_tweets.name)
                pid = os.getpid()
                logger.critical("Killing collecting process: %d" % pid)
                os.kill(pid, 1)

    def startCollection(self):
        try:
            logger.debug("starting datasift client")
            self.ds_client.start_stream_subscriber()
            logger.info("datasift client started")
        except Exception as e:
            logger.exception("exception in DataCollector.startCollection%s" % str(e))
            self.stopCollection()
            raise e

    def stopCollection(self):
        try:
            logger.debug("stoping datasift client")
            # self.current_file_for_received_tweets.close() # cannot be closed here as it was opened in a different process
            if self.ds_client._stream_process.is_alive():
                logger.debug("stream process is alive, trying to terminate")
                logger.debug("self.ds_client._stream_process is %s" % "None" if self.ds_client._stream_process is None else "not None: "+str(self.ds_client._stream_process))
                self.ds_client._stream_process.terminate()
            logger.info("datasift client stoped")
        except Exception as e:
            logger.exception("exception in DataCollector.stopCollection%s" % str(e))
            raise e

    def isCollecting(self):  # maybe rather use self.ds_client._stream_process.join()
        return self.ds_client._stream_process.is_alive()

    def nTweetsReceivedInPast24h(self):
        try:
            return self.ds_client.usage('day')['streams'][self.stream]['licenses']['twitter']
        except Exception as e:
            logger.exception("exception %s" % str(e))
            raise e

def json_date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

def main():
    try:
        logger.info("Initializing data collection")
        dataCollector = DataCollector(ds_username, ds_api_key, csdl, data_path)
        dataCollector.startCollection()
        while dataCollector.isCollecting():
            logger.debug("dataCollector is collecting, main process sleeps")
            time.sleep(60)
    except (KeyboardInterrupt):
        logger.warning("SIGINT signal received. Stoping collection.")
    except Exception as e:
        logger.exception("exception %s" % str(e))
        raise e
    finally:
        logger.info("Stoping data collection")
        dataCollector.stopCollection()
        logger.info("done.")


if __name__ == '__main__':
    main()
