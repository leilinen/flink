package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperator;

/**
 * SqlHCumulateTableFunction implements an operator for hcumulative.
 *
 * <p>It allows four parameters:
 *
 * <ol>
 *   <li>a table
 *   <li>a descriptor to provide a time attribute column name from the input table
 *   <li>an interval parameter to specify the window size to increase.
 *   <li>an interval parameter to specify the max length of window size
 * </ol>
 */
public class SqlHCumulateTableFunction extends SqlWindowTableFunction {

    public SqlHCumulateTableFunction() {
        super("HCUMULATE", new OperandMetadataImpl());
    }

    /** Operand type checker for HCUMULATE. */
    private static class OperandMetadataImpl extends AbstractOperandMetadata {
        OperandMetadataImpl() {
            super(
                    ImmutableList.of(
                            PARAM_DATA, PARAM_TIMECOL, PARAM_STEP, PARAM_SIZE, PARAM_OFFSET),
                    4);
        }

        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
            if (!checkTableAndDescriptorOperands(callBinding, 1)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }
            if (!checkIntervalOperands(callBinding, 2)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }
            if (callBinding.getOperandCount() == 5) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure);
            }
            // check time attribute
            return throwExceptionOrReturnFalse(
                    checkTimeColumnDescriptorOperand(callBinding, 1), throwOnFailure);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
            return opName
                    + "(TABLE table_name, DESCRIPTOR(timecol), "
                    + "datetime interval, datetime interval)";
        }
    }
}
