package com.example.demo.batchconfig;

import com.example.demo.entity.BankTransaction;
import com.example.demo.repository.BankTransationRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class ItemWriterConfig implements ItemWriter<BankTransaction> {
    @Autowired
    private BankTransationRepository bankTransationRepository;
    @Override
    public void write(List<? extends BankTransaction> list) throws Exception {
        bankTransationRepository.saveAll(list);
    }
}
