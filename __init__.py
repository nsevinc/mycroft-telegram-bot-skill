import logging
from mycroft import MycroftSkill, intent_file_handler

import telegram
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from telegram.error import TelegramError

from mycroft_bus_client import MessageBusClient, Message
import time
import pandas as pd
import os.path

# Enable logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logger = logging.getLogger(__name__)


class DataFrameHelper:
    def __init__(self, path='', table="",indexcol="",datecols=[]):
        self.mydateparser = lambda x: pd.datetime.strptime(x , "%Y-%m-%d")
        
# =============================================================================
#       Data File Conf.
# =============================================================================
        self.data_file_ext = 'dat'
        self.data_file_path = path
# =============================================================================

        self.datecols = datecols
        self.tablename = table
        self.indexcol = indexcol
        
        self.data = self.CheckAndLoadOrCreateCSV(table,indexcol,datecols,self.mydateparser)
        self.columnnames = self.DFGetColumnNames(table)
        
    def CheckAndLoadOrCreateCSV(self, table="",indexcol="",datecols=[],mydateparser=""):
        if os.path.isfile(f'{self.data_file_path}{table}.{self.data_file_ext}'):
            return pd.read_csv(filepath_or_buffer=f'{self.data_file_path}{table}.{self.data_file_ext}',
                                    index_col=indexcol, 
                                    parse_dates = datecols, 
                                    date_parser = mydateparser
                                    )
        else:
            return self.DFSetColumns(table)
        
    def LoadDataFrameForNewRow(self):
        if os.path.isfile(f'{self.data_file_path}{self.tablename}.{self.data_file_ext}'):
            return pd.read_csv(filepath_or_buffer=f'{self.data_file_path}{self.tablename}.{self.data_file_ext}',
                               parse_dates = self.datecols, 
                               date_parser = self.mydateparser
                               )
        else:
            return self.DFSetColumns(self.tablename)
    
    def DFGetRecord(self,row='',col=''):
        try:
            if col != '':
                return self.data.loc[row,col]
            else:
                return self.data.loc[row,:]
        except:
            return None
        
    def DFGetRecordAll(self):
        return self.data.loc[:,:]
    
    def DFGetColumnNames(self,tablename=''):
        if tablename == 'TelegramContactList':
            return ['name','telegram']
        else:
            return None
        
    def DFSetColumns(self,tablename=''):
        if self.DFGetColumnNames(tablename) is not None:
            column_names = self.DFGetColumnNames(tablename)
            return pd.DataFrame(columns=column_names)
        else:
            return None
        
    def DFAppendRow(self,row=[[]]):
        dt = self.LoadDataFrameForNewRow()
        
        dd = pd.DataFrame(row,columns=self.DFGetColumnNames(self.tablename))
        # dd['birthday'] = pd.to_datetime(dd['birthday'])
        
        dt = dd.append(dt, ignore_index=True, sort=False)
        dt.set_index(self.indexcol,inplace=True)
        # dt['birthday'] = pd.to_datetime(dt['birthday'])       
        
        dt.to_csv(f'{self.data_file_path}{self.tablename}.{self.data_file_ext}')
        
    def DFisEmpty(self):
        if self.data.index.max() == None:
            return True
        else:
            return False
    
class MycroftTelegramBot(MycroftSkill):
    def __init__(self):
        super(MycroftTelegramBot, self).__init__(name="MycroftTelegramBot")
    
    def initialize(self):
        self.bottoken = self.settings.get('bottoken','')
        
        if self.bottoken == '':
            self.speak_dialog("no.settings")
            
        self.receiverid = self.settings.get('adminchatid', '')
        self.person_chatid  = None
        self.path = self.file_system.path +'/'
        
        
        self.last_result = ''
        self.send_next = 0
        self.send_last = 0
        self.add_event('speak', self.send_speak_handler)
        
        try:
            self.updater = Updater(self.bottoken, use_context=True)
            dp = self.updater.dispatcher
        
            dp.add_handler(CommandHandler("start", self.start))
            dp.add_handler(CommandHandler("ask", self.ask))
            dp.add_handler(CommandHandler("register", self.register))
            dp.add_handler(CommandHandler("chatid", self.getChatid))
        
            dp.add_handler(MessageHandler(Filters.text, self.toMyCroft))
        
            dp.add_error_handler(self.error)
        
            self.updater.start_polling()            
        except TelegramError as err:            
             logger.error('Update "%s" caused error', err)           

    def register(self,update,context):
        if self.IsChatIdAdmin(update.message.chat_id) == False: #only the admin has permission
            update.message.reply_text(self.translate("access.denied"))
        else:
            try:
                
                arguments = update.message.text.split(" ",2)
                
                user = arguments[2]
                telegram = arguments[1]
                
                self.addUser(user,telegram)
                update.message.reply_text(f'Kayıt İsmi : {user} \n'+
                                          f'Telegram ID : {telegram}\n'+
                                          'Kayıt İşlemi Başarıyla Tamamlandı.')
            except:
                update.message.reply_text(self.translate('missing.parameters'))
    
    def getChatid(self, update, context):
        update.message.reply_text('Chat ID =>')
        update.message.reply_text(f'{update.message.chat_id}')
        
    def start(self, update, context):
        if self.IsChatIdInList(update.message.chat_id) == True or self.IsChatIdAdmin(update.message.chat_id) == True:
            update.message.reply_text(self.translate("bot.welcome.message"))
    
    def ask(self, update, context):
        if self.IsChatIdInList(update.message.chat_id) == True or self.IsChatIdAdmin(update.message.chat_id) == True:
            
            if self.checkSettings == False: return
            
            try:
                mesaj = update.message.text.split(" ",1)
                
                self.person_chatid = update.message.chat_id
                self.send_next = 1
                self.add_event('telegram-integration:response', self.send_speak_handler)
    
                self.bus.emit(Message("recognizer_loop:utterance", {"utterances": [mesaj[1]] }))
            except:
                update.message.reply_text(self.translate('missing.parameters'))
        
    
    def toMyCroft(self, update, context):
        if self.IsChatIdInList(update.message.chat_id) == True or self.IsChatIdAdmin(update.message.chat_id) == True:
            logger.info('Setting up client to connect to a local mycroft instance')
            client = MessageBusClient()
            client.run_in_thread()
            
            logger.info('Sending speak message...')
            client.emit(Message('speak', data={'utterance': f"{update.message.text}"}))
        
    
    def error(self, update, context):
        logger.error('Update "%s" caused error "%s"', update, context.error)    
   
    def responseHandler(self, message):
        response = message.data.get("utterance")
        self.bus.emit(Message("telegram-integration:response", {"utterance": response }))
        time.sleep(1)
        self.remove_event('telegram-integration:response')
        
    def send_speak_handler(self, message):
        sendData = message.data.get("utterance")
        if self.send_next == 1:
            sendbot = telegram.Bot(token=self.bottoken)
            sendbot.send_message(chat_id=self.person_chatid, text=sendData)
            self.person_chatid = None
            self.send_next = 0
        else:
            self.last_result = sendData
            
    def addUser(self, name='', telegramid =''):
        df = DataFrameHelper(self.path,'TelegramContactList','name')     
        data = [[name, telegramid]]         
        df.DFAppendRow(data)
    
    def IsChatIdAdmin(self,telegramid=''):
        if str(telegramid) == str(self.receiverid):
            return True
        else:
            return False
        
    def IsChatIdInList(self,telegramid=''):
        df = DataFrameHelper(self.path,'TelegramContactList','telegram',[])     
        if telegramid in df.data.index:
            return True
        else:
            return False
        
    def checkSettings(self):
        if self.bottoken == '':
            self.speak_dialog("no.settings")
            return False
        else:
            return True
        
    def shutdown(self): # shutdown routine
        try:
            if self.updater is not None:
                self.updater.stop() # will stop update and dispatcher
                self.updater.is_idle = False
        except:
            pass
        finally:
            super(MycroftTelegramBot, self).shutdown()
        
    @intent_file_handler('send.last.result.intent')
    def handle_last_result_telegram(self, message):
        
        if self.checkSettings == False: return
        
        try:
            if self.last_result != '':
                self.person_chatid = self.receiverid
                sendbot = telegram.Bot(token=self.bottoken)
                sendbot.send_message(chat_id=self.person_chatid, text=self.last_result)    
            else:
                self.speak_dialog("dont.know")
        except:
            logger.error(self.translate("error"))
        
    @intent_file_handler('send.next.result.intent')
    def handle_test_telegram(self, message):
        
        if self.checkSettings == False: return
        
        person = message.data.get('person')
        
        if person is not None:
            try:
                df = DataFrameHelper(self.path,'TelegramContactList','name')           
                self.person_chatid = df.DFGetRecord(person,'telegram')
                
                if self.person_chatid is None:
                    self.speak_dialog('next.result.sent.toadmin')
                    self.person_chatid = self.receiverid
                else:
                    self.speak_dialog('next.result.sent',data={'person':person})
                    self.person_chatid = str(self.person_chatid)
                    
            except:
                logger.error(self.translate("error"))
        else:
            self.person_chatid = self.receiverid
            self.speak_dialog('next.result.sent.toadmin')
            
        
        time.sleep(1)
        self.send_next = 1
        self.add_event('telegram-integration:response', self.send_speak_handler)


def create_skill():
    return MycroftTelegramBot()

