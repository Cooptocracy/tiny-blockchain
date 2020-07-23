require 'digest'

class Node
  VALUE_FROM_MINING = 1
  NETWORK_ID = 'network'
  NODE_ID = 'this node'

  def initialize(pow_generator=POWGenerator.new)
    @chain = Chain.init
    @transactions = []
    @peers = []
    @pow_generator = pow_generator
  end

  def submit_transaction(tx)
    @transactions << tx
  end

  def mine
    submit_transaction(mining_trx)

    @chain.add_block({proof_of_work: proof_of_work, transactions: @transactions})
    broadcast_changes
  end

  def chain
    @chain
  end

  def merge_chain(another_chain)
    @chain = another_chain.clone
  end

  def add_peer(peer_node)
    @peers << peer_node
  end

  def consensus_method=(method)
    @consensus = method
  end

  private

  def proof_of_work
    @pow_generator.pow
  end

  def mining_trx
    {from: NETWORK_ID,
      to: NODE_ID,
      amount: VALUE_FROM_MINING}
  end

  def broadcast_changes
    @peers.each { |peer| peer.merge_chain(@chain) }
  end

end

class POWGenerator
  def pow(number)
    increment = next_interation(number)

    while(valid_pow?(increment)) do
      increment = next_interation(increment)
    end

    increment
  end

  private

  def valid_pow?(n)
    n % 9 != 0
  end

  def next_interation(n)
    n + 1
  end
end

class Chain

  def self.init
    Chain.new
  end

  def add_block(data)
    @tail = Block.build_from(@tail, data)
    @blocks = @blocks + 1
  end

  def initialize
    @tail = Block.build_genesis
    @blocks = 1
  end

  def last_block
    @tail
  end

  def block_count
    @blocks
  end
end

Block = Struct.new(:index, :data, :prev_hash) do
  def hash
    Digest::SHA256.hexdigest("#{index}#{timestamp}#{data}#{prev_hash}")
  end

  def self.build_from(block, data)
    Block.new(block.index+1, data, block.hash)
  end

  def self.build_genesis
    Block.new(0, 'Genesis Block', "0")
  end

  def timestamp
    @timestamp || Timestamp.new
  end

  def timestamp=(timestamp)
    @timestamp = timestamp
  end

  def to_s
    "Block #{index}. Hash #{hash}"
  end
end

class Timestamp
  def initialize
    @t = Time.now
  end
  def to_s
    @t.to_s
  end
end


def run
  chain = Chain.init
  (0..9).each { |e|
    chain.add_block("Block #{e}")
    puts chain.last_block
  }
end

run
