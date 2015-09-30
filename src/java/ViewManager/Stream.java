package ViewManager;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import com.datastax.driver.core.Row;

public class Stream implements Serializable {

	//JoinAggGroupBy
	private CustomizedRow innerJoinAggGroupByOldRow;
	private CustomizedRow innerJoinAggGroupByUpdatedOldRow;
	private CustomizedRow innerJoinAggGroupByNewRow;
	private CustomizedRow innerJoinAggGroupByDeleteRow;
	private CustomizedRow leftOrRightJoinAggGroupByOldRow;
	private CustomizedRow leftOrRightJoinAggGroupByUpdatedOldRow;
	private CustomizedRow leftOrRightJoinAggGroupByNewRow;
	private CustomizedRow leftOrRightJoinAggGroupByDeleteRow;

	//JoinAgg
	private CustomizedRow innerJoinAggOldRow;
	private CustomizedRow innerJoinAggUpdatedOldRow;
	private CustomizedRow innerJoinAggNewRow;
	private CustomizedRow innerJoinAggDeleteRow;
	private CustomizedRow leftOrRightJoinAggOldRow;
	private CustomizedRow leftOrRightJoinAggUpdatedOldRow;
	private CustomizedRow leftOrRightJoinAggNewRow;
	private CustomizedRow leftOrRightJoinAggDeleteRow;

	//Delta
	private CustomizedRow deltaUpdatedRow;
	private CustomizedRow deltaDeletedRow;

	//Preaggregation
	private CustomizedRow updatedPreaggRow;
	private CustomizedRow updatedPreaggRowDeleted;
	private CustomizedRow updatedPreaggRowChangeAK;
	//Preaggregation delete
	private CustomizedRow deletePreaggRow;
	private CustomizedRow deletePreaggRowDeleted;

	//Reverse Join
	private CustomizedRow reverseJoinUpdateNewRow;
	private CustomizedRow reverseJoinUpadteOldRow;
	private CustomizedRow reverseJoinUpdatedOldRow_changeJoinKey;
	private CustomizedRow reverseJoinDeleteNewRow;
	private CustomizedRow revereJoinDeleteOldRow;
	
	private String baseTable;
	
	private boolean isDeleteOperation;


	public Stream() {
		resetJoinAggGroupByUpRows();
		resetJoinAggRows();
		resetDeltaRows();
		resetPreaggregationRows();
		resetReverseJoinRows();
	}


	public void resetReverseJoinRows() {
		reverseJoinUpdateNewRow = null;
		reverseJoinUpadteOldRow = null;
		reverseJoinUpdatedOldRow_changeJoinKey= null;
		reverseJoinDeleteNewRow= null;
		revereJoinDeleteOldRow = null;
	}

	public void resetPreaggregationRows() {
		updatedPreaggRow = null;
		updatedPreaggRowDeleted = null;
		updatedPreaggRowChangeAK = null;
		deletePreaggRow = null;
		deletePreaggRowDeleted = null;

	}

	public void resetDeltaRows() {
		deltaUpdatedRow = null;
		deltaDeletedRow = null;
	}

	public void resetJoinAggRows() {

		innerJoinAggOldRow = null;
		innerJoinAggUpdatedOldRow = null;
		innerJoinAggNewRow = null;
		innerJoinAggDeleteRow = null;
		leftOrRightJoinAggOldRow = null;
		leftOrRightJoinAggUpdatedOldRow = null;
		leftOrRightJoinAggNewRow = null;
		leftOrRightJoinAggDeleteRow = null;

	}

	public CustomizedRow getInnerJoinAggGroupByOldRow() {
		return innerJoinAggGroupByOldRow;
	}

	public void setInnerJoinAggGroupByOldRow(CustomizedRow joinAggGroupByOldRow) {
		this.innerJoinAggGroupByOldRow = joinAggGroupByOldRow;
	}

	public CustomizedRow getInnerJoinAggGroupByUpdatedOldRow() {
		return innerJoinAggGroupByUpdatedOldRow;
	}

	public void setInnerJoinAggGroupByUpdatedOldRow(
			CustomizedRow joinAggGroupByUpdatedOldRow) {
		this.innerJoinAggGroupByUpdatedOldRow = joinAggGroupByUpdatedOldRow;
	}

	public CustomizedRow getInnerJoinAggGroupByNewRow() {
		return innerJoinAggGroupByNewRow;
	}

	public void setInnerJoinAggGroupByNewRow(CustomizedRow joinAggGroupByNewRow) {
		this.innerJoinAggGroupByNewRow = joinAggGroupByNewRow;
	}

	public CustomizedRow getLeftOrRightJoinAggGroupByOldRow() {
		return leftOrRightJoinAggGroupByOldRow;
	}

	public void setLeftOrRightJoinAggGroupByOldRow(
			CustomizedRow leftOrRightJoinAggGroupByOldRow) {
		this.leftOrRightJoinAggGroupByOldRow = leftOrRightJoinAggGroupByOldRow;
	}

	public CustomizedRow getLeftOrRightJoinAggGroupByNewRow() {
		return leftOrRightJoinAggGroupByNewRow;
	}

	public void setLeftOrRightJoinAggGroupByNewRow(
			CustomizedRow leftOrRightJoinAggGroupByNewRow) {
		this.leftOrRightJoinAggGroupByNewRow = leftOrRightJoinAggGroupByNewRow;
	}

	public CustomizedRow getLeftOrRightJoinAggGroupByUpdatedOldRow() {
		return leftOrRightJoinAggGroupByUpdatedOldRow;
	}

	public void setLeftOrRightJoinAggGroupByUpdatedOldRow(
			CustomizedRow leftOrRightJoinAggGroupByUpdatedOldRow) {
		this.leftOrRightJoinAggGroupByUpdatedOldRow = leftOrRightJoinAggGroupByUpdatedOldRow;
	}

	public void resetJoinAggGroupByUpRows(){
		setInnerJoinAggGroupByOldRow(null);
		setInnerJoinAggGroupByUpdatedOldRow(null);
		setInnerJoinAggGroupByNewRow(null);
		setInnerJoinAggGroupByDeleteOldRow(null);
		setLeftOrRightJoinAggGroupByNewRow(null);
		setLeftOrRightJoinAggGroupByOldRow(null);
		setLeftOrRightJoinAggGroupByUpdatedOldRow(null);
		setLeftOrRightJoinAggGroupByDeleteRow(null);
	}

	public CustomizedRow getInnerJoinAggGroupByDeleteOldRow() {
		return innerJoinAggGroupByDeleteRow;
	}

	public void setInnerJoinAggGroupByDeleteOldRow(
			CustomizedRow innerJoinAggGroupByDeleteRow) {
		this.innerJoinAggGroupByDeleteRow = innerJoinAggGroupByDeleteRow;
	}

	public CustomizedRow getLeftOrRightJoinAggGroupByDeleteRow() {
		return leftOrRightJoinAggGroupByDeleteRow;
	}

	public void setLeftOrRightJoinAggGroupByDeleteRow(
			CustomizedRow leftOrRightJoinAggGroupByDeleteRow) {
		this.leftOrRightJoinAggGroupByDeleteRow = leftOrRightJoinAggGroupByDeleteRow;
	}

	public CustomizedRow getInnerJoinAggOldRow() {
		return innerJoinAggOldRow;
	}

	public void setInnerJoinAggOldRow(CustomizedRow innerJoinAggGroupOldRow) {
		this.innerJoinAggOldRow = innerJoinAggGroupOldRow;
	}

	public CustomizedRow getInnerJoinAggUpdatedOldRow() {
		return innerJoinAggUpdatedOldRow;
	}

	public void setInnerJoinAggUpdatedOldRow(
			CustomizedRow innerJoinAggGroupUpdatedOldRow) {
		this.innerJoinAggUpdatedOldRow = innerJoinAggGroupUpdatedOldRow;
	}

	public CustomizedRow getInnerJoinAggNewRow() {
		return innerJoinAggNewRow;
	}

	public void setInnerJoinAggNewRow(CustomizedRow innerJoinAggGroupNewRow) {
		this.innerJoinAggNewRow = innerJoinAggGroupNewRow;
	}

	public CustomizedRow getInnerJoinAggDeleteRow() {
		return innerJoinAggDeleteRow;
	}

	public void setInnerJoinAggDeleteRow(CustomizedRow innerJoinAggGroupDeleteRow) {
		this.innerJoinAggDeleteRow = innerJoinAggGroupDeleteRow;
	}

	public CustomizedRow getLeftOrRightJoinAggOldRow() {
		return leftOrRightJoinAggOldRow;
	}

	public void setLeftOrRightJoinAggOldRow(
			CustomizedRow leftOrRightJoinAggGroupOldRow) {
		this.leftOrRightJoinAggOldRow = leftOrRightJoinAggGroupOldRow;
	}

	public CustomizedRow getLeftOrRightJoinAggUpdatedOldRow() {
		return leftOrRightJoinAggUpdatedOldRow;
	}

	public void setLeftOrRightJoinAggUpdatedOldRow(
			CustomizedRow leftOrRightJoinAggGroupUpdatedOldRow) {
		this.leftOrRightJoinAggUpdatedOldRow = leftOrRightJoinAggGroupUpdatedOldRow;
	}

	public CustomizedRow getLeftOrRightJoinAggNewRow() {
		return leftOrRightJoinAggNewRow;
	}

	public void setLeftOrRightJoinAggNewRow(
			CustomizedRow leftOrRightJoinAggGroupNewRow) {
		this.leftOrRightJoinAggNewRow = leftOrRightJoinAggGroupNewRow;
	}

	public CustomizedRow getLeftOrRightJoinAggDeleteRow() {
		return leftOrRightJoinAggDeleteRow;
	}

	public void setLeftOrRightJoinAggDeleteRow(
			CustomizedRow leftOrRightJoinAggGroupDeleteRow) {
		this.leftOrRightJoinAggDeleteRow = leftOrRightJoinAggGroupDeleteRow;
	}

	public CustomizedRow getDeltaUpdatedRow() {
		return deltaUpdatedRow;
	}

	public void setDeltaUpdatedRow(CustomizedRow deltaUpdatedRow) {
		this.deltaUpdatedRow = deltaUpdatedRow;
	}

	public CustomizedRow getDeltaDeletedRow() {
		return deltaDeletedRow;
	}

	public void setDeltaDeletedRow(CustomizedRow deltaDeletedRow) {
		this.deltaDeletedRow = deltaDeletedRow;
	}

	public CustomizedRow getUpdatedPreaggRow() {
		return updatedPreaggRow;
	}

	public void setUpdatedPreaggRow(CustomizedRow updatedPreaggRow) {
		this.updatedPreaggRow = updatedPreaggRow;
	}

	public CustomizedRow getUpdatedPreaggRowDeleted() {
		return updatedPreaggRowDeleted;
	}

	public void setUpdatedPreaggRowDeleted(CustomizedRow updatedPreaggRowDeleted) {
		this.updatedPreaggRowDeleted = updatedPreaggRowDeleted;
	}

	public CustomizedRow getUpdatedPreaggRowChangeAK() {
		return updatedPreaggRowChangeAK;
	}

	public void setUpdatedPreaggRowChangeAK(CustomizedRow updatedPreaggRowChangeAK) {
		this.updatedPreaggRowChangeAK = updatedPreaggRowChangeAK;
	}

	public CustomizedRow getReverseJoinUpdateNewRow() {
		return reverseJoinUpdateNewRow;
	}

	public void setReverseJoinUpdateNewRow(CustomizedRow reverseJoinUpdateNewRow) {
		this.reverseJoinUpdateNewRow = reverseJoinUpdateNewRow;
	}

	public CustomizedRow getReverseJoinUpadteOldRow() {
		return reverseJoinUpadteOldRow;
	}

	public void setReverseJoinUpadteOldRow(CustomizedRow reverseJoinUpadteOldRow) {
		this.reverseJoinUpadteOldRow = reverseJoinUpadteOldRow;
	}

	public CustomizedRow getReverseJoinUpdatedOldRow_changeJoinKey() {
		return reverseJoinUpdatedOldRow_changeJoinKey;
	}

	public void setReverseJoinUpdatedOldRow_changeJoinKey(
			CustomizedRow reverseJoinUpdatedOldRow_changeJoinKey) {
		this.reverseJoinUpdatedOldRow_changeJoinKey = reverseJoinUpdatedOldRow_changeJoinKey;
	}

	public CustomizedRow getReverseJoinDeleteNewRow() {
		return reverseJoinDeleteNewRow;
	}

	public void setReverseJoinDeleteNewRow(CustomizedRow reverseJoinDeleteNewRow) {
		this.reverseJoinDeleteNewRow = reverseJoinDeleteNewRow;
	}

	public CustomizedRow getRevereJoinDeleteOldRow() {
		return revereJoinDeleteOldRow;
	}

	public void setRevereJoinDeleteOldRow(CustomizedRow revereJoinDeleteOldRow) {
		this.revereJoinDeleteOldRow = revereJoinDeleteOldRow;
	}


	public CustomizedRow getDeletePreaggRow() {
		return deletePreaggRow;
	}

	public void setDeletePreaggRow(CustomizedRow deletePreaggRow) {
		this.deletePreaggRow = deletePreaggRow;
	}

	public CustomizedRow getDeletePreaggRowDeleted() {
		return deletePreaggRowDeleted;
	}

	public void setDeletePreaggRowDeleted(CustomizedRow deletePreaggRowDeleted) {
		this.deletePreaggRowDeleted = deletePreaggRowDeleted;
	}
	
	public String getBaseTable() {
		return baseTable;
	}

	public void setBaseTable(String baseTable) {
		this.baseTable = baseTable;
	}
	
	public boolean isDeleteOperation() {
		return isDeleteOperation;
	}

	public void setDeleteOperation(boolean isDeleteOperation) {
		this.isDeleteOperation = isDeleteOperation;
	}

}
