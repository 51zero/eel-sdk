package io.eels.datastream;

public enum AddMode {
    // creates the field if it does not exist, throws an error if the field already exists
    Add,
    // creates the field if it does not exist, replaces the field if it already exists
    Replace,
    // creates the field if it does not exist, or does nothing if the field already exists
    Offer
}
