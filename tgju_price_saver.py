import asyncio
import aiohttp
import pymongo
import logging
import datetime
import time
from telethon import TelegramClient
import re
#import logging information from information file
from specs import id,hash,admins


#introduce mongo db 
mongoclient = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongoclient['tgju_prices']


#making collections
bitcoin_col = db['bitcoin']  
etherium_col = db['etherium']
doller_azad_col = db['doller azad']
euro_azad_col = db['euro azad']
gold_18_col = db['18 karat gold']
sekke_emamy_col = db['sekke ememy']
sekke_bahar_col = db['sekke bahar azady']
aluminium_col = db['aluminium(UK)']
lead_UK_col = db['lead(UK)']
roy_col = db['roy(UK)']
copper_UK_col = db['copper(UK)']
copper_USA_col = db['copper(USA)']
wheat_USA_col = db['wheat(UK)']
datalog = db['datalog']  
  
 
async def main() : 
    
    
    #sign in telegram   
    client=TelegramClient('user',id,hash)
    await client.start()
        
        
    #introduce aiohttp client and sucket tgju
    async with aiohttp.ClientSession() as session :
        ws = await session.ws_connect('wss://stream.tgju.org/connection/websocket')    
        await ws.send_str('{"params":{"name":"js"},"id":1}')
        await ws.send_str('{"method":1,"params":{"channel":"tgju:stream"},"id":2}')
        

        async for msg in ws : 
            
                #Conversion data from str to list
                if type(msg.data) == str :
                    list = msg.data.split('|')
                    #filter useless data
                    if list[0] != '' :
                        try : 
                    
                            #aranging data and save them in database
                            if list[2] == '' or list[2] == 'crypto-bitcoin-irr'\
                                or list[2] == 'crypto-avalanche-irr' or list[0][-13 : ] == 'tolerance_low'  or len(list) == 1 :
                                None     
                                                       
                            else :
                                #extraction delay
                                if 'crypto-bitcoin' or 'crypto-ethereum' or 'price_dollar_rl' or 'price_eur' or 'geram18' \
                                    or 'retail_sekee' or 'retail_sekeb' or 'aluminium' or 'lead' or 'general_6' or \
                                        'general_7' or 'copperp' or 'general_15' in list :
                                    deltas = []
                                    for msg in list :
                                        time = re.findall('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',msg)
                                        if time != [] :
                                            time = time[0]
                                            real_datetime = datetime.datetime.now()                                     
                                            deltas.append(abs(real_datetime - datetime.datetime.strptime(f'{time}' , '%Y-%m-%d %H:%M:%S')))
                                            
                                    message_datetime = real_datetime
                                    delta = min(deltas)
                                    if delta >= abs(datetime.datetime.strptime('00:05:00' , '%H:%M:%S')\
                                        - datetime.datetime.strptime('00:00:00' , '%H:%M:%S')) :
                                        
                                        my_dict = {'the problem' : 'we have a big delay' , 'time' : datetime.datetime.now()}
                                        datalog.insert_one(my_dict)   

                                        for admin in admins :
                                            await client.send_message(admin,'#delay /n ⚠️⚠️⚠️⚠️⚠️ /n we have a big delay \
                                                /n ⚠️⚠️⚠️⚠️⚠️')                                 
                                    
                                    ###getting prices
                                    ##cryptocurrencies
                                    #bitcoin                                
                                    if 'crypto-bitcoin' in list :                                    
                                        #filtering outlier data 
                                        try : 
                                            bitcoin_price = float(list[list.index('crypto-bitcoin')+4])
                                            my_dict = {'price($)' : bitcoin_price , 'time' : message_datetime}
                                            bitcoin_col.insert_one(my_dict)
                                                    
                                        except ValueError :
                                            my_dict = {'the problem' : 'bitcoin unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)   
                                    
                                    #etherium             
                                    if 'crypto-ethereum' in list :
                                        
                                        #filtering outlier data 
                                        try : 
                                            etherium_price = float(list[list.index('crypto-ethereum')+4])
                                            my_dict = {'price($)' : etherium_price , 'time' : message_datetime}
                                            etherium_col.insert_one(my_dict)
                                                    
                                        except ValueError :
                                            my_dict = {'the problem' : 'etherium unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict) 
                                        
                                            
                                    ##money
                                    #doller(azad)
                                    if 'price_dollar_rl' in list :
                                        
                                        #filtering string data
                                        try :
                                            doller_azad_price = float(list[list.index('price_dollar_rl')+4])
                                            my_dict = {'price(ريال)' : doller_azad_price , 'time' : message_datetime}
                                            doller_azad_col.insert_one(my_dict)
                                            
                                        except ValueError :
                                            my_dict = {'the problem' : 'dollar azad unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)                                        
                                    
                                    #euro(azad)
                                    if 'price_eur' in list :
                                        
                                        #filtering string data
                                        try : 
                                            euro_azad_price = float(list[list.index('price_eur')+4])
                                            my_dict = {'price(ريال)' : euro_azad_price , 'time' : message_datetime}
                                            euro_azad_col.insert_one(my_dict)
                                            
                                        except ValueError :
                                            my_dict = {'the problem' : 'euro azad unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)  
                                        
                                                    
                                    ##gold
                                    #18 karat gold(each gram)
                                    if 'geram18' in list :
                                        
                                        #filtering string data
                                        try :
                                            gold_18_price = float(list[list.index('geram18')+4])
                                            my_dict = {'price(ريال)' : gold_18_price , 'time' : message_datetime}
                                            gold_18_col.insert_one(my_dict)
                                            
                                        except ValueError :
                                            my_dict = {'the problem' : '18 karat gold unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)  
                                        
                                                                                
                                    ##sekke
                                    #sekke emamy   
                                    if 'retail_sekee' in list :
                                                                                
                                        #filtering string data
                                        try :
                                            sekke_emamy_price = float(list[list.index('retail_sekee')+4]) 
                                            my_dict = {'price(ريال)' : sekke_emamy_price , 'time' : message_datetime}
                                            sekke_emamy_col.insert_one(my_dict)
                                            
                                        except ValueError :
                                            my_dict = {'the problem' : 'sekke emamy unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)      
                                                                            
                                    #sekke bahar azadi
                                    if 'retail_sekeb' in list :
                                        
                                        #filtering string data
                                        try :                                        
                                            sekke_bahar_price = list[list.index('retail_sekeb')+4]
                                            my_dict = {'price(ريال)' : sekke_bahar_price , 'time' : message_datetime}
                                            sekke_bahar_col.insert_one(my_dict)
                                            
                                        except ValueError :
                                            my_dict = {'the problem' : 'sekke bahar azadi unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)  
                                    
                                                                                
                                    ##Goods
                                    #aluminium England
                                    if 'aluminium' in list :

                                        #filtering string data
                                        try :                                                  
                                            alu_UK_price = float(list[list.index('aluminium')+4])  
                                            my_dict = {'price($)' : alu_UK_price , 'time' : message_datetime}
                                            aluminium_col.insert_one(my_dict)
                                            
                                        except ValueError :
                                            my_dict = {'the problem' : 'aluminium England unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)     
                                                                                
                                    #lead England
                                    if 'lead' in list :

                                        #filtering string data
                                        try :                                                     
                                            lead_UK_price = float(list[list.index('lead')+4])
                                            my_dict = {'price($)' : lead_UK_price , 'time' : message_datetime}
                                            lead_UK_col.insert_one(my_dict)
                                            
                                        except ValueError :
                                            my_dict = {'the problem' : 'lead England unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)     
                                                                                                                            
                                    #roy England  
                                    if 'general_6' in list :

                                        #filtering string data
                                        try :                                         
                                            roy_UK_price = float(list[list.index('general_6')+4])  
                                            my_dict = {'price($)' : roy_UK_price , 'time' : message_datetime}
                                            roy_col.insert_one(my_dict)     
                                                                                
                                        except ValueError :
                                            my_dict = {'the problem' : 'roy England unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)  
                                                                                    
                                    #copper England    
                                    if 'general_7' in list :
                                        #filtering string data
                                        try :                                         
                                            copper_UK_price = float(list[list.index('general_7')+4])  
                                            my_dict = {'price($)' : copper_UK_price , 'time' : message_datetime}
                                            copper_UK_col.insert_one(my_dict)   
                                                                                
                                        except ValueError :
                                            my_dict = {'the problem' : 'copper England unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)    
                                                                                    
                                    #copper USA    
                                    if 'copperp' in list :
                                        #filtering string data
                                        try :      
                                            copper_USA_price = float(list[list.index('copperp')+4])                             
                                            my_dict = {'price($)' : copper_USA_price , 'time' : message_datetime}
                                            copper_USA_col.insert_one(my_dict)  
                                                                         
                                        except ValueError :
                                            my_dict = {'the problem' : 'copper USA unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict)       
                                                                                    
                                    #Wheat USA
                                    if 'general_15' in list :
                                        
                                        #filtering string data
                                        try :                                         
                                            Wheat_USA_price = float(list[list.index('general_15')+4])                       
                                            my_dict = {'price($)' : Wheat_USA_price , 'time' : message_datetime}
                                            wheat_USA_col.insert_one(my_dict)
                                                         
                                        except ValueError :
                                            my_dict = {'the problem' : 'wheat USA unknown datum' , 'time' : datetime.datetime.now()}
                                            datalog.insert_one(my_dict) 

                        #handling error of some lists that are empty or has only 1 datum 
                        except IndexError :
                            None
time_counter = datetime.datetime.now()                        
while datetime.datetime.now() - time_counter < abs(datetime.datetime.strptime('00:01:00' , '%H:%M:%S')\
                                        - datetime.datetime.strptime('00:00:00' , '%H:%M:%S')) :
                         
    try :
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
        
    #handling error of disconnection
    except (ConnectionError , RuntimeWarning) :
        my_dict = {'the problem' : ' Connection to Telegram failed 5 time(s)' , 'time' : datetime.datetime.now()}
        datalog.insert_one(my_dict)
        logging.basicConfig(format = '%(asctime)s-%(filename)s-%(message)s' , filename = 'error.log')
        logging.exception('!!!!!!!!!!!disconnection!!!!!!!!!')
        #try every 10 second
        time.sleep(4)
        
    except  (TimeoutError , OSError) :
        print('noooooooooooooooooooooooooo')
        my_dict = {'the problem' : 'disconnection' , 'time' : datetime.datetime.now()}
        datalog.insert_one(my_dict)
        logging.basicConfig(format = '%(asctime)s-%(filename)s-%(message)s' , filename = 'error.log')
        logging.exception('!!!!!!!!!!!disconnection!!!!!!!!!')

my_dict = {'the problem' : 'long disconnection' , 'time' : datetime.datetime.now()}
datalog.insert_one(my_dict)
logging.basicConfig(format = '%(asctime)s-%(filename)s-%(message)s' , filename = 'error.log')
logging.exception('!!!!!!!!!!!the project stopped!!!!!!!!!')                            