#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>

using namespace db;

Delete::Delete(TransactionId t, DbIterator *child) {
    // TODO pa3.3: some code goes here

}

const TupleDesc &Delete::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    static TupleDesc td({Types::INT_TYPE}, {"insertCount"});
    return td;
}

void Delete::open() {
    // TODO pa3.3: some code goes here
    child->open();
    Operator::open();
}

void Delete::close() {
    // TODO pa3.3: some code goes here
    child->close();
    Operator::close();
}

void Delete::rewind() {
    // TODO pa3.3: some code goes here
    child->rewind();
    hasBeenCalled = false;
}

std::vector<DbIterator *> Delete::getChildren() {
    // TODO pa3.3: some code goes here
    return {child};
}

void Delete::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    if (!children.empty()) {
        child = children[0];
    }
}

std::optional<Tuple> Delete::fetchNext() {
    // TODO pa3.3: some code goes here
}
