import { BlockChain } from './BlockChain';
import { Block } from './block';


var TinyCoin = new BlockChain(5);

TinyCoin.addBlock(new Block(1,{user:"Genesis Account 1",Amount:1000000}));
TinyCoin.addBlock(new Block(2,{user:"Genesis Account 2",Amount:1000000}));
TinyCoin.addBlock(new Block(3,{user:"Genesis Account 3",Amount:1000000}));
TinyCoin.addBlock(new Block(4,{user:"Genesis Account 4",Amount:1000000}));
TinyCoin.addBlock(new Block(5,{user:"Genesis Account 5",Amount:1000000}));



console.log(JSON.stringify(TinyCoin,null,4));

console.log("Chains are synced : "+TinyCoin.checkBlockChain());
