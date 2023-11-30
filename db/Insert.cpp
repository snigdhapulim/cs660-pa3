#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;

std::optional<Tuple> Insert::fetchNext() {
    // TODO pa3.3: some code goes here
    if (hasBeenCalled) {
        return {}; // Return empty if called more than once
    }
    hasBeenCalled = true;

    // Insert tuples read from child into the table
    while (child->hasNext()) {
        //Database::getBufferPool().insertTuple(t, tableId, child->next());
        insertCount++;
    }

    // Return a one-field tuple containing the number of inserted records
    TupleDesc td = TupleDesc({Types::INT_TYPE}, {"insertCount"});
    Tuple resultTuple(td);
    resultTuple.setField(0, new IntField(insertCount));
    return resultTuple;
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId) : t(t), child(child), tableId(tableId), insertCount(0), hasBeenCalled(false) {
    // TODO pa3.3: some code goes here
}

const TupleDesc &Insert::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    static TupleDesc td({Types::INT_TYPE}, {"insertCount"});
    return td;
}

void Insert::open() {
    // TODO pa3.3: some code goes here
    child->open();
    Operator::open();
}

void Insert::close() {
    // TODO pa3.3: some code goes here
    child->close();
    Operator::close();
}

void Insert::rewind() {
    // TODO pa3.3: some code goes here
    child->rewind();
    hasBeenCalled = false;
}

std::vector<DbIterator *> Insert::getChildren() {
    // TODO pa3.3: some code goes here
    return {child};
}

void Insert::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    if (!children.empty()) {
        child = children[0];
    }
}
