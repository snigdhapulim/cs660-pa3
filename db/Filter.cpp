#include <db/Filter.h>
#include <iostream>

using namespace db;

Filter::Filter(Predicate p, DbIterator *child) {
    // TODO pa3.1: some code goes here
    this->predicate = &p;
    this->child = child;
}

Predicate *Filter::getPredicate() {
    // TODO pa3.1: some code goes here
    return predicate;
}

const TupleDesc &Filter::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return child->getTupleDesc();
}

void Filter::open() {
    // TODO pa3.1: some code goes here
    child->open();
}

void Filter::close() {
    // TODO pa3.1: some code goes here
    child->close();
}

void Filter::rewind() {
    // TODO pa3.1: some code goes here
    child->rewind();
}

std::vector<DbIterator *> Filter::getChildren() {
    // TODO pa3.1: some code goes here
    std::vector<DbIterator *> children;
    children.push_back(child);
    return children;
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    assert(children.size() == 1);
    child = children[0];
}

std::optional<Tuple> Filter::fetchNext() {

    std::cout << 'is it in the open function?' << std::endl;
    // TODO pa3.1: some code goes here
    while (child->hasNext()) {
        std::optional<Tuple> tuple = child->next();
        if (predicate->filter(tuple.value())) {
            return tuple;
        }
    }
    return std::nullopt;


}
