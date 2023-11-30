#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {
    // TODO pa3.2: some code goes here
    if (!aggIterator) {
        aggIterator = aggregator->iterator();
        aggIterator->open();
    }

    // Continue fetching from the aggregation iterator.
    if (aggIterator->hasNext()) {
        return aggIterator->next();
    }
    return {};
}

Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop)
        : child(child), afield(afield), gfield(gfield), aop(aop){
    // TODO pa3.2: some code goes here

    if (gfield == Aggregator::NO_GROUPING) {
        // Create a TupleDesc for when there is no grouping
        tupleDesc = TupleDesc({child->getTupleDesc().getFieldType(afield)},
                              {aggregateFieldName()});
    } else {
        // Create a TupleDesc for when there is grouping
        tupleDesc = TupleDesc({child->getTupleDesc().getFieldType(gfield),
                               child->getTupleDesc().getFieldType(afield)},
                              {groupFieldName(),
                               aggregateFieldName()});
    }
}

int Aggregate::groupField() {
    // TODO pa3.2: some code goes here
    return gfield;
}

std::string Aggregate::groupFieldName() {
    // TODO pa3.2: some code goes here
    if (gfield == Aggregator::NO_GROUPING) {
        return ""; // Return empty string if there is no grouping
    } else {
        return child->getTupleDesc().getFieldName(gfield); // Return the name of the group field
    }
}

int Aggregate::aggregateField() {
    // TODO pa3.2: some code goes here
    return afield;
}

std::string Aggregate::aggregateFieldName() {
    // TODO pa3.2: some code goes here
    return child->getTupleDesc().getFieldName(afield);
}

Aggregator::Op Aggregate::aggregateOp() {
    // TODO pa3.2: some code goes here
    return aop;
}

void Aggregate::open() {
    // TODO pa3.2: some code goes here
    child->open();
    Operator::open();
}

void Aggregate::rewind() {
    // TODO pa3.2: some code goes here
    child->rewind();
    if (aggIterator != nullptr) {
        aggIterator->rewind();
    }
}

const TupleDesc &Aggregate::getTupleDesc() const {
    // TODO pa3.2: some code goes here
    return tupleDesc;
}

void Aggregate::close() {
    // TODO pa3.2: some code goes here
    child->close();
    if (aggIterator) {
        aggIterator->close();
    }
    Operator::close();
}

std::vector<DbIterator *> Aggregate::getChildren() {
    // TODO pa3.2: some code goes here
    return {child};
}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.2: some code goes here
    if (!children.empty()) {
        child = children[0];
    }
}
