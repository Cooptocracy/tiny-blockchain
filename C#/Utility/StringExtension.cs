using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlockChain.Utility
{
    public static class StringExtension
    {
        public static string GenerateStringForProofWork(this string str, int difficulty)
        {
            StringBuilder strBuilder = new StringBuilder(str, difficulty);            
            return strBuilder.ToString();
        }        
    }
}
