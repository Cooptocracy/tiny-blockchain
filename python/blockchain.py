import hashlib
import datetime as date
import time

class Util(object):
    @staticmethod
    def datetime2timestamp(date_time):
        return time.mktime(date_time.timetuple())
    pass

class Encrypt(object):
    @staticmethod
    def hash_encrypt(data_str):
         sha = hashlib.sha256()
         sha.update(data_str)
         return sha.hexdigest()

class Block(object):
    index = None 
    previous_hash = None
    timestamp = None
    data = None 
    hash = None

    def create_start_block(self,index=0,previous_hash="",data="the block for gods"):        
        self.index = index
        self.previous_hash = previous_hash
        self.timestamp = date.datetime.now()
        self.data = data
        self.hash = Encrypt.hash_encrypt((str(self.index)+str(self.previous_hash)+str(self.timestamp)+str(self.data)).encode("utf-8"))
        pass

    def create_new_block(self,previous_block,data):
        self.index = previous_block.index + 1
        self.previous_hash = previous_block.hash
        self.timestamp = date.datetime.now()
        self.data = data
        self.hash = Encrypt.hash_encrypt((str(self.index)+str(self.previous_hash)+str(self.timestamp)+str(self.data)).encode("utf-8"))
        pass
    
    def __str__(self):
        return "index:"+str(self.index)+"\nprevious_hash:"+self.previous_hash+"\ntimestamp:"+str(self.timestamp)+"\ndata:"+self.data+"\nhash:"+self.hash+"\n\n"
        pass

class BlockCreater(object):
    @staticmethod
    def create_start_block(cls): 
        block = Block() 
        block.create_start_block()
        return block

    @staticmethod
    def create_new_block(cls,previous_block,data="new trade data"):
        block = Block()
        block.create_new_block(previous_block,data)
        return block
        


class BlockChain(object):
    block_chains=[]

    def create_blockchain(self):
        self.block_chains.append(BlockCreater.create_start_block(BlockCreater))

    
    def add_new_block(self,data):
        while True:
            tblock = BlockCreater.create_new_block(BlockCreater,self.block_chains[-1],data)
            if Common_Recognise_POW.pow(tblock,self.block_chains[-1]):
                break
        print(Util.datetime2timestamp(tblock.timestamp) - Util.datetime2timestamp(self.block_chains[-1].timestamp))
        print("Level"：\n",Common_Recognise_POW.level)
        print("Difficulty"：\n",Common_Recognise_POW.difficulty)
        print("Block":\n",tblock)
        self.block_chains.append(tblock)
        return tblock
        

    def get_full_block_chains(self):
        return self.block_chains
    
    def __str__(self):
        content = ""
        for item in self.block_chains:
            content = content + str(item) + "\n"
        return content



class Common_Recognise(object):
        pass
    
class Common_Recognise_POW(Common_Recognise):
    level = 0
    difficulty = ""
    generate_time = 10
    @staticmethod
    def pow(new_block,previous_block):
        flag = new_block.hash.startswith(Common_Recognise_POW.difficulty)
        if flag == True:
            Common_Recognise_POW.difficulty = ""
            for i in range(0,Common_Recognise_POW.level):
                Common_Recognise_POW.difficulty = Common_Recognise_POW.difficulty + "0"
            time_dis = (Util.datetime2timestamp(new_block.timestamp) - Util.datetime2timestamp(previous_block.timestamp))
                
            if time_dis  < Common_Recognise_POW.generate_time:
                Common_Recognise_POW.level = Common_Recognise_POW.level + 1
                pass
            elif time_dis  > Common_Recognise_POW.generate_time:
                Common_Recognise_POW.level = Common_Recognise_POW.level - 1        
                pass
        return flag
        pass

    
if __name__ == '__main__':
    demo = BlockChain()
    demo.create_blockchain()
    for i in range(1,20):
        demo.add_new_block(""+str(i)+"")
    print(demo)
    pass
