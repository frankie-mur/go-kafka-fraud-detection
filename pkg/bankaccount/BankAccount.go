package bankaccount

import (
	"fmt"
	"github.com/bxcodec/faker/v4"
	"github.com/frankie-mur/go-kafka/pkg/models"
)

func GenerateBankAccount() (*models.BankAccount, error) {
	bankAccount := models.BankAccount{}
	err := faker.FakeData(&bankAccount)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bank account: %w", err)
	}
	return &bankAccount, nil
}
