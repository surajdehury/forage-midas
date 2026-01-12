package com.jpmc.midascore.kafka;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Optional;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRepository;

    public TransactionListener(UserRepository userRepository,
                               TransactionRecordRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {

        // 1️⃣ Validate sender & recipient exist
        Optional<UserRecord> senderOpt =
                userRepository.findById(transaction.getSenderId());

        Optional<UserRecord> recipientOpt =
                userRepository.findById(transaction.getRecipientId());

        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            return; // invalid transaction
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        // 2️⃣ Convert amount
        BigDecimal amount = BigDecimal.valueOf(transaction.getAmount());

        // 3️⃣ Check sufficient balance
        if (sender.getBalance().compareTo(amount) < 0) {
            return; // insufficient funds
        }

        // 4️⃣ Update balances
        sender.setBalance(sender.getBalance().subtract(amount));
        recipient.setBalance(recipient.getBalance().add(amount));

        userRepository.save(sender);
        userRepository.save(recipient);

        // 5️⃣ Persist transaction record
        TransactionRecord record =
                new TransactionRecord(sender, recipient, amount);

        transactionRepository.save(record);

        // 🔍 TEMP DEBUG FOR TASK 3 (REMOVE AFTER ANSWER)
        userRepository.findByUsername("waldorf").ifPresent(w ->
                System.out.println("FINAL WALDORF BALANCE = " + w.getBalance())
        );
    }
}
