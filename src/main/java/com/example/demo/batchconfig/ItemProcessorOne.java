package com.example.demo.batchconfig;

import com.example.demo.entity.BankTransaction;
import org.springframework.batch.item.ItemProcessor;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class ItemProcessorOne implements ItemProcessor<BankTransaction,BankTransaction> {

    // Ce processor nous permet de transformer le format data string to date format
    private DateFormat dateFormat=new SimpleDateFormat("dd/MM/yyyy-HH:mm");
    @Override
    public BankTransaction process(BankTransaction bankTransaction) throws Exception {
        bankTransaction.setTransactionDate(dateFormat.parse(bankTransaction.getStrTransactionDate()));
        return bankTransaction;
    }

}
