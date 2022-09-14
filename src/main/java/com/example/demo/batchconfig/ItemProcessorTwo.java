package com.example.demo.batchconfig;

import com.example.demo.entity.BankTransaction;
import lombok.Getter;
import org.springframework.batch.item.ItemProcessor;

public class ItemProcessorTwo implements ItemProcessor<BankTransaction,BankTransaction> {

    // Ce processor nous permet de calculer le totalDebit et TotalCredit
    @Getter private double totalDebit;
    @Getter private double totalCredit;

    @Override
    public BankTransaction process(BankTransaction bankTransaction) throws Exception {

        if(bankTransaction.getTransactionType().equals("D")) totalDebit += bankTransaction.getTransactionAmount();
        else if(bankTransaction.getTransactionType().equals("C")) totalCredit += bankTransaction.getTransactionAmount();

        return bankTransaction;
    }
}
