#include "block_verify.h"

unsigned char calculate_hash(unsigned char *str1, unsigned char *str2) {
    int i;  

    for ( i = 0; i<SHA256_DIGEST_LENGTH; i++) {
    	if (str1[i] != str2[i]) 
    		return 0;
    }
    return 1;
}

void pr_hash(unsigned char hash[], int hashlen){
	int i;

	for (i=0; i<hashlen; i++){
		printf("%02x\n",hash[i]);
	}
}

void verifychain(){
	int count = 1;

	if (block_head == NULL){
		printf("\n");
		return;
	}

	struct block_data *curr_blk = block_head->next_block; 
 	struct block_data *prehash = block_head;

 	while(curr_blk)
 	{	
 		int i;
 		unsigned char b[i];

 		printf("%d\n[%d]\t", count++, curr_blk->blkdata);
		sha256_hash(b[i], convert_str(*prehash), sizeof(*prehash));
 		pr_hash(b[i], SHA256_DIGEST_LENGTH);
		printf(" === ");
		pr_hash(curr_blk->prehash, SHA256_DIGEST_LENGTH);
	
		if(calculate_hash(b[i], curr_blk->prehash)){
			printf("\n");
			return;
		}else{
  			printf("\n");
  		}
		prehash = curr_blk;
		curr_blk = curr_blk->next_block;
	}
}
