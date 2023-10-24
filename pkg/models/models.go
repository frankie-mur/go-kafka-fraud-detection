package models

type BankAccount struct {
	FirstName        string  `faker:"first_name"`
	LastName         string  `faker:"last_name"`
	CreditCardNumber string  `faker:"cc_number"`
	Amount           float64 `faker:"amount"`
}
