package main.parsetree;

/**
 * LQP Node class for scan operation
 * @attr: tableName: String - name of tableName to be scanned
 */
public class ScanNode extends LQPNode {
    private String tableName;

    public ScanNode(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return String.format("⊟(%s)", this.tableName == null ? "" : this.tableName);
    }

    /**
     * Read the tableName and return memory address for the loaded tableName.
     * getChild() is undefined.
     */
    public String execute() {
        return tableName;
    }
}
