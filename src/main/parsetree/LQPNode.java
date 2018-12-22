package main.parsetree;

import java.util.ArrayList;

/**
 * LQP Node represents all different types of operations/nodes in our Logic Query Plan tree
 * These can be projection, selection, joins, set operations etc.
 * The inherited classes will contain all the necessary parameters that the operation needs,
 * as well as the method implementation.
 * The `next` parameter is the operation that is followed hierarchically in the LQP tree.
 * The output of executing `next` is the input to `this`.
 * For example, scan's output used as an input to projection.
 */
public abstract class LQPNode {
    private ArrayList<LQPNode> children;

    public ArrayList<LQPNode> getChildren() {
        return children;
    }

    /**
     * @return first child - useful shortcut, as many operations only have one child
     */
    public LQPNode getChild() {
        return children.get(0);
    }

    public void setChildren(ArrayList<LQPNode> children) {
        this.children = children;
    }

    public void setChild(LQPNode child) {
        this.children = new ArrayList<LQPNode>();
        this.children.add(child);
    }

    public void setChild(int idx, LQPNode child){
        this.children.set(idx, child);
    }

    public void addChild(LQPNode child) {
        if (this.children == null) {
            this.children = new ArrayList<LQPNode>();
        }
        this.children.add(child);
    }
}
