  
#include "block.h"
#include "openssl/crypto.h"

unsigned char calculate_hash(unsigned char *str1, unsigned char *str2);
void pr_hash(unsigned char hash[], int hashlen);
void verifychain();
