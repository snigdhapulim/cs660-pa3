#include <db/IntegerAggregator.h>
#include <db/IntField.h>

using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
    // TODO pa3.2: some code goes here
    std::unordered_map<Field *, int>::const_iterator current;
    std::unordered_map<Field *, int>::const_iterator end;
    const TupleDesc &td;
    bool isOpen;
    std::unordered_map<Field *, int> counts;

public:
    IntegerAggregatorIterator(int gbfield,
                              const TupleDesc &td,
                              const std::unordered_map<Field *, int> &count) : td(td), counts(counts), isOpen(false) {
        // TODO pa3.2: some code goes here
    }

    void open() override {
        // TODO pa3.2: some code goes here
        current = counts.begin();
        end = counts.end();
        isOpen = true;
    }

    bool hasNext() override {
        // TODO pa3.2: some code goes here
        return isOpen && current != end;
    }

    Tuple next() override {
        // TODO pa3.2: some code goes here
        if (!hasNext()) {
            throw std::runtime_error("No more tuples");
        }
        // Create a new tuple with the appropriate schema
        Tuple tuple(td);
        // Set the fields of the tuple based on the current iterator position
        tuple.setField(0, new IntField(*static_cast<const IntField*>(current->first))); // Group value
        tuple.setField(1, new IntField(current->second)); // Aggregate value
        // Move to the next position
        ++current;
        // Return the created tuple
        return tuple;
    }

    void rewind() override {
        // TODO pa3.2: some code goes here
        if (!isOpen) {
            throw std::runtime_error("Iterator not open");
        }
        // Reset the current position to the beginning of the map
        current = counts.begin();
    }

    const TupleDesc &getTupleDesc() const override {
        // TODO pa3.2: some code goes here
        return td;
    }

    void close() override {
        // TODO pa3.2: some code goes here
        isOpen = false;
    }
};

IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Aggregator::Op what)
                                     : gbfield(gbfield), gbfieldtype(gbfieldtype), afield(afield), what(what), noGroupAggregate(0)
                                     {
    // TODO pa3.2: some code goes here
    if (gbfield == NO_GROUPING) {
        noGroupAggregate = 0; // or the initial value based on the operation
    } else {
        groups.clear(); // Ensure the groups map is empty
    }
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    // TODO pa3.2: some code goes here

}

DbIterator *IntegerAggregator::iterator() const {
    // TODO pa3.2: some code goes here
}