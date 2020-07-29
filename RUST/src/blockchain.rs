use super::*;
use ed25519_dalek::PublicKey;

#[derive(Debug)]
pub struct Blockchain {
    blocks: Vec<Block>,
    unmined_transactions: Vec<Transaction>,
    mining_reward: f32,
}

impl Blockchain {
    pub fn new() -> Self {
        Blockchain {
            blocks: vec![],
            unmined_transactions: vec![],
            mining_reward: MINING_REWARD,
        }
    }

    pub fn mine_unmined_transactions(&mut self, miner_address: PublicKey) {
        self.unmined_transactions.push(Transaction {
            sender: None,
            receiver: Some(miner_address),
            amount: self.mining_reward,
            signature: None,
        });

        let transactions = &self.unmined_transactions;
        let mut block = Block::new(transactions.to_vec());
        match self.blocks.last() {
            Some(pre_block) => block.set_pre_hash(pre_block.hash.to_owned()),
            None => block.set_pre_hash("0".to_string()), 
        }
        block.set_hash();

        block.mine();

        self.blocks.push(block);

        self.unmined_transactions = Vec::new();
    }

    pub fn add_new_transaction(&mut self, transaction: Transaction) {
        if transaction.sender.is_none() || transaction.receiver.is_none() {
            panic!("Transaction sender and receiver must have valid address!!! ")
        }

        if !transaction.is_valid_transaction() {
            panic!("Transaction must be valid!!")
        }

        self.unmined_transactions.push(transaction);
    }

    pub fn is_valid_chain(&self) -> bool {
        let blocks = &self.blocks;

        for (i, block) in blocks.iter().enumerate() {
            if block.hash
                != calculate_hash(
                    &block.pre_hash,
                    &block.transaction,
                    &block.timestamp,
                    &block.nonce,
                )
            {
                return false;
            }
            if i > 0 && block.pre_hash != blocks[i - 1].hash {
                return false;
            }

            if !block.has_valid_transactions() {
                return false;
            }
        }

        return true;
    }
}
