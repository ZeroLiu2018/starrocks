// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RequiredPropertyDeriver extends PropertyDeriverBase<Void, ExpressionContext> {
    private final ColumnRefFactory columnRefFactory;
    private final PhysicalPropertySet requirementsFromParent;
    private List<List<PhysicalPropertySet>> requiredProperties;

    public RequiredPropertyDeriver(TaskContext context) {
        this.requirementsFromParent = context.getRequiredProperty();
        this.columnRefFactory = context.getOptimizerContext().getColumnRefFactory();
    }

    public List<List<PhysicalPropertySet>> getRequiredProps(GroupExpression groupExpression) {
        requiredProperties = Lists.newArrayList();
        groupExpression.getOp().accept(this, new ExpressionContext(groupExpression));
        return requiredProperties;
    }

    @Override
    public Void visitOperator(Operator node, ExpressionContext context) {
        List<PhysicalPropertySet> requiredProps = new ArrayList<>();
        for (int childIndex = 0; childIndex < context.arity(); ++childIndex) {
            // @todo: resolve required gather property by check child limit
            if (!node.hasLimit() && context.getChildOperator(childIndex).hasLimit()) {
                requiredProps.add(createLimitGatherProperty(context.getChildOperator(childIndex).getLimit()));
            } else {
                requiredProps.add(PhysicalPropertySet.EMPTY);
            }
        }
        requiredProperties.add(requiredProps);
        return null;
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        // 1 For broadcast join
        PhysicalPropertySet rightBroadcastProperty =
                new PhysicalPropertySet(new DistributionProperty(DistributionSpec.createReplicatedDistributionSpec()));
        requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY, rightBroadcastProperty));

        JoinHelper joinHelper = JoinHelper.of(node, context.getChildOutputColumns(0), context.getChildOutputColumns(1));

        if (joinHelper.onlyBroadcast()) {
            return null;
        }

        if (joinHelper.onlyShuffle()) {
            requiredProperties.clear();
        }

        // 2 For shuffle join
        List<Integer> leftOnPredicateColumns = joinHelper.getLeftOnColumns();
        List<Integer> rightOnPredicateColumns = joinHelper.getRightOnColumns();
        Preconditions.checkState(leftOnPredicateColumns.size() == rightOnPredicateColumns.size());
        requiredProperties.add(computeShuffleJoinRequiredProperties(requirementsFromParent, leftOnPredicateColumns,
                rightOnPredicateColumns));

        return null;
    }

    @Override
    public Void visitPhysicalMergeJoin(PhysicalMergeJoinOperator node, ExpressionContext context) {
        //prepare sort property
        ColumnRefSet leftChildColumns = context.getChildOutputColumns(0);
        ColumnRefSet rightChildColumns = context.getChildOutputColumns(1);

        JoinHelper joinHelper = JoinHelper.of(node, leftChildColumns, rightChildColumns);

        List<Integer> leftOnPredicateColumns = joinHelper.getLeftOnColumns();
        List<Integer> rightOnPredicateColumns = joinHelper.getRightOnColumns();
        List<Ordering> leftOrderings = leftOnPredicateColumns.stream()
                .map(l -> new Ordering(columnRefFactory.getColumnRef(l), true, true)).collect(Collectors.toList());

        List<Ordering> rightOrderings = rightOnPredicateColumns.stream()
                .map(l -> new Ordering(columnRefFactory.getColumnRef(l), true, true)).collect(Collectors.toList());

        SortProperty leftSortProperty = new SortProperty(new OrderSpec(leftOrderings));
        SortProperty rightSortProperty = new SortProperty(new OrderSpec(rightOrderings));

        // 1 For broadcast join
        PhysicalPropertySet leftBroadcastProperty = new PhysicalPropertySet(leftSortProperty);
        PhysicalPropertySet rightBroadcastProperty =
                new PhysicalPropertySet(new DistributionProperty(DistributionSpec.createReplicatedDistributionSpec()),
                        rightSortProperty);
        requiredProperties.add(Lists.newArrayList(leftBroadcastProperty, rightBroadcastProperty));

        if (joinHelper.onlyBroadcast()) {
            return null;
        }

        if (joinHelper.onlyShuffle()) {
            requiredProperties.clear();
        }

        // 2 For shuffle join
        Preconditions.checkState(leftOnPredicateColumns.size() == rightOnPredicateColumns.size());
        List<PhysicalPropertySet> physicalPropertySets =
                computeShuffleJoinRequiredProperties(requirementsFromParent, leftOnPredicateColumns,
                        rightOnPredicateColumns);
        physicalPropertySets.get(0).setSortProperty(leftSortProperty);
        physicalPropertySets.get(1).setSortProperty(rightSortProperty);
        requiredProperties.add(physicalPropertySets);

        return null;
    }

    @Override
    public Void visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, ExpressionContext context) {
        // If scan tablet sum leas than 1, do one phase local aggregate is enough
        if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() == 0
                && context.getRootProperty().isExecuteInOneTablet()
                && node.getType().isGlobal() && !node.isSplit()) {
            requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
            return null;
        }

        LogicalOperator child = (LogicalOperator) context.getChildOperator(0);
        // If child has limit, we need to gather data to one instance
        if (child.hasLimit() && (node.getType().isGlobal() && !node.isSplit())) {
            requiredProperties.add(Lists.newArrayList(createLimitGatherProperty(child.getLimit())));
            return null;
        }

        if (!node.getType().isLocal()) {
            List<Integer> columns = node.getPartitionByColumns().stream().map(ColumnRefOperator::getId).collect(
                    Collectors.toList());

            // None grouping columns
            if (columns.isEmpty()) {
                requiredProperties.add(Lists.newArrayList(createGatherPropertySet()));
                return null;
            }

            // shuffle aggregation
            requiredProperties.add(
                    Lists.newArrayList(computeAggRequiredShuffleProperties(requirementsFromParent, columns)));
            return null;
        }

        requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
        return null;
    }

    @Override
    public Void visitPhysicalTopN(PhysicalTopNOperator topN, ExpressionContext context) {
        LogicalOperator child = (LogicalOperator) context.getChildOperator(0);
        // If child has limit, we need to gather data to one instance
        if (child.hasLimit() && (topN.getSortPhase().isFinal() && !topN.isSplit())) {
            PhysicalPropertySet requiredProperty = createLimitGatherProperty(child.getLimit());
            requiredProperties.add(Lists.newArrayList(requiredProperty));
        } else {
            requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY));
        }

        return null;
    }

    @Override
    public Void visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
        requiredProperties.add(Lists.newArrayList(createGatherPropertySet()));
        return null;
    }

    @Override
    public Void visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
        List<Integer> partitionColumnRefSet = new ArrayList<>();

        node.getPartitionExpressions().forEach(e -> partitionColumnRefSet
                .addAll(Arrays.stream(e.getUsedColumns().getColumnIds()).boxed().collect(Collectors.toList())));

        SortProperty sortProperty = new SortProperty(new OrderSpec(node.getEnforceOrderBy()));

        DistributionProperty distributionProperty;
        if (partitionColumnRefSet.isEmpty()) {
            distributionProperty = new DistributionProperty(DistributionSpec.createGatherDistributionSpec());
        } else {
            distributionProperty = createShuffleAggProperty(partitionColumnRefSet);
        }
        requiredProperties.add(Lists.newArrayList(new PhysicalPropertySet(distributionProperty, sortProperty)));

        return null;
    }

    @Override
    public Void visitPhysicalUnion(PhysicalUnionOperator node, ExpressionContext context) {
        processSetOperationChildProperty(context);
        return null;
    }

    @Override
    public Void visitPhysicalExcept(PhysicalExceptOperator node, ExpressionContext context) {
        processSetOperationChildProperty(context);
        return null;
    }

    @Override
    public Void visitPhysicalIntersect(PhysicalIntersectOperator node, ExpressionContext context) {
        processSetOperationChildProperty(context);
        return null;
    }

    private void processSetOperationChildProperty(ExpressionContext context) {
        List<PhysicalPropertySet> childProperty = new ArrayList<>();
        for (int i = 0; i < context.arity(); ++i) {
            LogicalOperator child = (LogicalOperator) context.getChildOperator(i);
            // If child has limit, we need to gather data to one instance
            if (child.hasLimit()) {
                childProperty.add(createLimitGatherProperty(child.getLimit()));
            } else {
                childProperty.add(PhysicalPropertySet.EMPTY);
            }
        }

        // Use Any to forbidden enforce some property, will add shuffle in FragmentBuilder
        requiredProperties.add(childProperty);
    }

    @Override
    public Void visitPhysicalLimit(PhysicalLimitOperator node, ExpressionContext context) {
        // limit node in Memo means that the limit cannot be merged into other nodes.
        requiredProperties.add(Lists.newArrayList(createLimitGatherProperty(node.getLimit())));
        return null;
    }

    @Override
    public Void visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
        requiredProperties.add(Lists.newArrayList(PhysicalPropertySet.EMPTY, requirementsFromParent));
        return null;
    }

    @Override
    public Void visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        requiredProperties.add(Lists.newArrayList(requirementsFromParent));
        return null;
    }
}
