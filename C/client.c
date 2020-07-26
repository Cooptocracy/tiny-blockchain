#include "block.h"


void blockchain_menu (struct block_data *block_head){
	int choise = 0, n = 0, whichIndex = 0;
	int inputnums = 0;
	float money = 0;

	printf("<>\t\n");
	printf("\n");
	printf("\n");

	scanf("%d",&choise);

	do{
		switch(choise){
			case 0:
				printf("");
				scanf("%s,%s,%f",&sender, &recvier, &money)
				trans(sender,recvier,money);
				break;
			case 1:
				printf("(create_block)");
				printf("");
				scanf("%d",&inputnums);
				create_block(inputnums);
				break;
			case 2:
				printf("(add_block)");
				scanf("%d", &whichIndex);
				add_block(whichIndex);
				printf("", whichIndex, LengthList());
				break;
			case 3:
				printf(":");
				pr_all_Blocks();
				break;
			case 4:
				printf(":");//
				verifychain();
				break;
			default:
				printf("！\n");
				break;
		}
	}while(1);
}

void empty_block_failed() {
	printf("Fail");
}
