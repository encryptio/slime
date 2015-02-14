// package multi provides a store.Store which redundantly stores information.
//
// It uses Reed-Solomon erasure coding for efficient storage, at the cost of
// having to refer to many inner stores to read and write data.
package multi
