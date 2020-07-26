#include "block.h"

unsigned char* convert_str(struct block_data *blk){
	unsigned char *str = malloc(sizeof(unsigned char)*sizeof(blk));

	memcpy(str, &blk, sizeof(blk));
	return str;
}

void create_block (int inputnums){
	struct block_data *block_head;

	static int index = 0;
	int id = 1;
	time_t t;
	time(&t);

	if (block_head == NULL){
		index += inputnums;
		block_head = malloc(sizeof(struct block_data));
  	sha256_hash(block_head->prehash, (unsigned char *)"", sizeof(""));
		block_head -> index = index;
		block_head -> id = id;
		block_head -> blkdata = inputnums;
		block_head -> timestamp = ctime(&t);
		file_write();
		return;
	}
}

void add_block (int whichIndex){
	int i = 0;
	struct block_data *block_head;
	time_t t;
	time(&t);

	if (block_head == NULL){
		empty_block_failed();
		return;
	}

	struct block_data *currentblock = block_head;

	while(currentblock->next_block){
		currentblock = currentblock->next_block;
	}
		
	struct block_data *new_block = malloc(sizeof(struct block_data));
	currentblock -> next_block = new_block;	
	new_block -> index = whichIndex;
	new_block -> id = i + 1;
	new_block -> blkdata = 	LengthList();
	new_block -> timestamp = ctime(&t);
	sha256_hash(new_block->prehash, (unsigned char *)convert_str(currentblock), sizeof(*currentblock));
	file_write();
}

void file_write(){
	FILE *fp;
	int i;


	fp=fopen("/home/blockchain/data.txt", "wb");
	if (fp == NULL){
		printf("\n");
		exit(0);
	}

	fwrite(&block_head, sizeof(block_head), 1, fp); 
	fclose(fp);
}

void pr_block (struct block_data *blk){

	printf("%p", blk);
	printf("Index:[%d]\n", block_head->index);
	printf("id:[%d]\n", block_head->id);
	pr_hash(blk->prehash, sizeof(blk->prehash));
	printf("timestamp:[%s]\n", blk->timestamp);
  printf("%p\n", block_head->next_block);
}

void pr_all_Blocks(){
	struct block_data *currentblk = block_head;
	int count = 0;

	while(currentblk)
	{	
		pr_block(currentblk);
		currentblk = currentblk->next_block;
	}
}

int LengthList()
{
	struct block_data *len;
    int length = 0;
    if (block_head->next_block == NULL){
    	return 0;
    }
    else{
    	len = block_head -> next_block;
    	while(len){
    		length++;
    		len = len->next_block;
    	} 
    	return length;
    }
}
