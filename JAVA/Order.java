package tiny.blockchain;

public class Order {
    public String account;
    public String details;
    public long total;
    
    public Order(String account, String details, long total) {
        this.account = account;
        this.details = details;
        this.total = total;
    }
    
    public String getString() {
        return String.format(
                "Account [%s] has ordered [%s], total [%d] ",
                this.account,
                this.details,
                this.total);
    }
}
