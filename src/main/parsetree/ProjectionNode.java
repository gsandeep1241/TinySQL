package main.parsetree;

import main.dbops.DbProcedures;
import storageManager.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * LQP Node class for projection operation
 * @attr: isAll: True when SELECT *
 * @attr: columns: array of column names
 */
public class ProjectionNode extends LQPNode {

    private List<String> columns;
    private Boolean isAll;

    public ProjectionNode(TreeNode sqlTree) {
        TreeNode selectList = sqlTree.getChildren().stream()
                .filter(tn -> "select-list".equals(tn.getValue()))
                .findAny()
                .orElse(null);

        if ("*".equals(selectList.getChildren().get(0).getValue())) {
            this.isAll = true;
            this.columns = new ArrayList<>();
        } else {
            this.isAll = false;
            columns = new ArrayList<>();
            List<TreeNode> columnNames = selectList.getChildren().stream()
                    .filter(tn -> !",".equals(tn.getValue()))
                    .collect(Collectors.toList());

            for(TreeNode columnName: columnNames){
                if(columnName.getChildren().size() == 1){
                    columns.add(columnName.getChildren().get(0).getChildren().get(0).getValue());
                }else{
                    columns.add(columnName.getChildren().get(0).getChildren().get(0).getValue() + "." +
                            columnName.getChildren().get(1).getChildren().get(0).getValue());
                }
            }
        }
    }

    public void update(){
        for(int i=0; i < columns.size(); i++){
            String col = columns.get(i);
            if(col.contains(".")){
                columns.set(i, col.split("\\.")[1]);
            }
        }
    }

    public List<String> getColumns() {
        return this.columns;
    }

    public Boolean getIsAll() {
        return this.isAll;
    }

    @Override
    public String toString() {
        return String.format(
                "∏(%s)%s", String.join(",", this.columns),
                this.isAll.equals(true) ? " ALL" : ""
        );
    }

    /**
     * This is the method where the actual operation is performed - physical execution.
     * It assumes that this node has already been initialized properly.
     * It uses .getChildren() or .getChild() methods to get LQP nodes lower in the tree.
     * `execute()` can be called on those nodes recursively to get initial table/s to operate on.
     * It may also use the initialized properties to perform the operations.
     * Finally it returns the relation name of the temporary relation it creates
     * Note that, the cost of a query is estimated by the sizes of intermediate relations
     * because these relations may need to be stored on disk. What we do is, we always store intermediate
     * relations on disk.
     * Somehow, we should avoid storing the output on disk.
     */
    public String execute(boolean isFirst, DbProcedures impl, List<Tuple> output) {
        LQPNode child = this.getChild();

        String relation = "";
        if(child instanceof SelectionNode){
            String tempRelation = ((SelectionNode) child).execute(impl);
            relation = impl.onePassProjection(tempRelation, columns, isFirst, output, isAll);
        } else if (child instanceof ScanNode){
            String tempRelation = ((ScanNode) child).execute();
            relation = impl.onePassProjection(tempRelation, columns, isFirst, output, isAll);
        } else if (child instanceof JoinNode) {
            String tempRelation = ((JoinNode) child).execute(false, impl, null);
            relation = impl.onePassProjection(tempRelation, columns, isFirst, output, isAll);
        }
        return relation;
    }
}
