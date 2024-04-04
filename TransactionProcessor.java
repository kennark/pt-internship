package com.playtech.assignment;

import java.io.*;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class TransactionProcessor {

    public static void main(final String[] args) throws IOException {
        List<User> users = TransactionProcessor.readUsers(Paths.get(args[0]));
        List<Transaction> transactions = TransactionProcessor.readTransactions(Paths.get(args[1]));
        List<BinMapping> binMappings = TransactionProcessor.readBinMappings(Paths.get(args[2]));

        List<Event> events = TransactionProcessor.processTransactions(users, transactions, binMappings);

        TransactionProcessor.writeBalances(Paths.get(args[3]), users);
        TransactionProcessor.writeEvents(Paths.get(args[4]), events);
    }

    private static List<User> readUsers(final Path filePath) throws IOException {
        List<User> usersList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath.toFile()))) {
            br.readLine(); // skip the first line which is CSV column names
            String line = br.readLine();

            while (line != null) {
                // Split and rename details
                String[] details = line.split(",");
                String userID = details[0];
                String userName = details[1];
                double balance = Double.parseDouble(details[2]);
                Locale locale = Locale.of("", details[3]);

                boolean frozen = details[4].equals("1"); // value is true if the string is equal to "1", otherwise false
                double minDeposit = Double.parseDouble(details[5]);
                double maxDeposit = Double.parseDouble(details[6]);
                double minWithdrawal = Double.parseDouble(details[7]);
                double maxWithdrawal = Double.parseDouble(details[8]);

                User user = new User(userID, userName, balance, locale, frozen, minDeposit, maxDeposit, minWithdrawal, maxWithdrawal);
                usersList.add(user);

                line = br.readLine();
            }
        }

        System.out.println("Users file read");
        return usersList;
    }

    private static List<Transaction> readTransactions(final Path filePath) throws IOException {
        List<Transaction> transactionsList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath.toFile()))) {
            br.readLine(); // skip the first line which is CSV column names
            String line = br.readLine();

            while (line != null) {
                // Split and rename details
                String[] details = line.split(",");
                String transactionID = details[0];
                String userID = details[1];
                String type = details[2];
                double amount = Double.parseDouble(details[3]);
                String method = details[4];
                String bankAccount = details[5];

                Transaction transaction = new Transaction(transactionID, userID, type, amount, method, bankAccount);
                transactionsList.add(transaction);

                line = br.readLine();
            }
        }

        System.out.println("Transactions file read");
        return transactionsList;
    }

    private static List<BinMapping> readBinMappings(final Path filePath) throws IOException {
        List<BinMapping> binMappingList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath.toFile()))) {
            br.readLine(); // skip the first line which is CSV column names
            String line = br.readLine();

            while (line != null) {
                // Split and rename details
                String[] details = line.split(",");
                String bankName = details[0];
                long range_from = Long.parseLong(details[1]);
                long range_to = Long.parseLong(details[2]);
                String type = details[3];
                Locale locale = Locale.of("", details[4]);

                BinMapping binMapping = new BinMapping(bankName, range_from, range_to, type, locale);
                binMappingList.add(binMapping);

                line = br.readLine();
            }
        }

        System.out.println("Bins file read");
        return binMappingList;
    }

    private static List<Event> processTransactions(final List<User> users, final List<Transaction> transactions, final List<BinMapping> binMappings) {
        Map<String, User> accountBelongsTo = new HashMap<>();

        List<Event> eventList = new ArrayList<>();
        List<String> transactionsList = new ArrayList<>();


        for (Transaction transaction : transactions) {
            // Information for creating a new Event object
            String transactionID = transaction.transactionID;
            String status = Event.STATUS_APPROVED;
            String message = "OK";

            User user = findUser(users, transaction.userID);


            // Code is checking all processing requirements
            if (transactionsList.contains(transactionID)) {
                message = "Transaction " + transactionID + " already processed (id non-unique)";
            } else if (user != null) {
                Locale accountCountry;
                Locale userCountry = user.locale;

                if (user.frozen) {
                    message = "User " + transaction.userID + "account is frozen";
                } else if (transaction.method.equals("TRANSFER")) {
                    if (!validateIBAN(transaction.account)) {
                        message = "Invalid iban " + transaction.account;
                    }

                    accountCountry = findBankCountry(transaction.account);
                    if (!userCountry.getCountry().equals(accountCountry.getCountry())) {
                        message = "Invalid account country " + accountCountry.getCountry() + "; expected " + userCountry.getCountry();
                    }
                } else if (transaction.method.equals("CARD")) {
                    String type = findCardType(binMappings, transaction.account);
                    if (!type.equals("DC")) {
                        message = "Only DC cards allowed; got " + type;
                    }

                    accountCountry = findCardCountry(binMappings, transaction.account);
                    if (!userCountry.getISO3Country().equals(accountCountry.getCountry())) {
                        message = "Invalid country " + accountCountry.getCountry() + "; expected " + userCountry.getCountry() + " (" + userCountry.getISO3Country() + ")";
                    }
                } else {
                    message = "Invalid payment method";
                }
                if (transaction.amount < 0) {
                    message = "Transaction amount negative: " + String.format("%.2f", transaction.amount);

                } else if (accountBelongsTo.containsKey(transaction.account) && !accountBelongsTo.get(transaction.account).equals(user)) {
                    message = "Account " + transaction.account + " is in use by other user";
                } else if (transaction.transactionType.equals("WITHDRAW")) {
                    if (transaction.amount < user.minimumAllowedWithdrawal) {
                        message = "Amount " + String.format("%.2f", transaction.amount) + " is under the withdraw limit of " + String.format("%.2f", user.minimumAllowedWithdrawal);
                    } else if (transaction.amount > user.maximumAllowedWithdrawal) {
                        message = "Amount " + String.format("%.2f", transaction.amount) + " is over the withdraw limit of " + String.format("%.2f", user.maximumAllowedWithdrawal);
                    } else if (transaction.amount > user.balance) {
                        message = "Not enough balance to withdraw " + String.format("%.2f", transaction.amount) + " - balance is too low at " + String.format("%.2f", user.balance);
                    } else if (!accountBelongsTo.containsKey(transaction.account) && transaction.method.equals("TRANSFER")) {
                        message = "Cannot withdraw with a new account " + transaction.account;
                    }
                } else if (transaction.transactionType.equals("DEPOSIT")) {
                    if (transaction.amount < user.minimumAllowedDeposit) {
                        message = "Amount " + String.format("%.2f", transaction.amount) + " is under the deposit limit of " + String.format("%.2f", user.minimumAllowedDeposit);
                    } else if (transaction.amount > user.maximumAllowedDeposit) {
                        message = "Amount " + String.format("%.2f", transaction.amount) + " is over the deposit limit of " + String.format("%.2f", user.maximumAllowedDeposit);
                    }
                } else {
                    message = "Invalid transaction type " + transaction.transactionType;
                }
            } else {
                message = "User " + transaction.userID + " not found in Users";
            }

            // From previous checks: if anything were to be wrong, the message would differ. If everything is valid, then the message is still OK
            if (message.equals("OK")) {
                accountBelongsTo.putIfAbsent(transaction.account, user);
                user.balance = user.balance + (transaction.transactionType.equals("DEPOSIT") ? transaction.amount : -transaction.amount);
            } else {
                // Message is different = something is wrong so decline
                status = Event.STATUS_DECLINED;
            }
            Event event = new Event(transactionID, status, message);
            transactionsList.add(transactionID);
            eventList.add(event);
        }

        System.out.println("Transactions processed");

        return eventList;
    }

    // Finds User from list with the ID
    private static User findUser(final List<User> users, String idToFind) {
        for (User user : users) {
            if (user.userID.equals(idToFind))
                return user;
        }
        return null;
    }

    // Returns CC or DC, credit/debit
    private static String findCardType(final List<BinMapping> binMapping, String card) {
        long firstTenNumbers = Long.parseLong(card.substring(0, 10));
        for (BinMapping mapping : binMapping) {
            if (mapping.range_from <= firstTenNumbers && mapping.range_to >= firstTenNumbers) {
                return mapping.type;
            }
        }
        throw new RuntimeException("No bin mapping found for card " + card);
    }

    // Returns country as a Locale object
    private static Locale findBankCountry(String account) {
        return Locale.of("", account.substring(0, 2).toUpperCase());
    }

    // Returns country as a Locale object
    private static Locale findCardCountry(final List<BinMapping> binMapping, String card) {
        long firstTenNumbers = Long.parseLong(card.substring(0, 10));
        for (BinMapping mapping : binMapping) {
            if (mapping.range_from <= firstTenNumbers && mapping.range_to >= firstTenNumbers) {
                return mapping.country;
            }
        }
        throw new RuntimeException("Bin mapping not found for card " + card);
    }

    // Returns true if correct iban, false otherwise
    private static boolean validateIBAN(String iban) {

        // Move the first 4 characters to the end
        iban = iban.substring(4) + iban.substring(0, 4);

        // Replace letters with numbers
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < iban.length(); i++) {
            char c = iban.charAt(i);
            if (Character.isDigit(c)) {
                sb.append(c);
            } else {
                sb.append(Character.getNumericValue(c));
            }
        }

        BigInteger bigInt = new BigInteger(sb.toString());
        BigInteger remainder = bigInt.mod(new BigInteger("97"));

        return remainder.intValue() == 1;
    }


    private static void writeBalances(final Path filePath, final List<User> users) throws IOException {
        try (FileWriter fileWriter = new FileWriter(filePath.toFile())) {
            fileWriter.write("USER_ID,BALANCE\n");
            for (User user : users) {
                fileWriter.write(user.userID + "," + user.balance + "\n");
            }
        }

    }


    private static void writeEvents(final Path filePath, final List<Event> events) throws IOException {
        try (final FileWriter writer = new FileWriter(filePath.toFile(), false)) {
            writer.append("transaction_id,status,message\n");
            for (final var event : events) {
                writer.append(event.transactionId).append(",").append(event.status).append(",").append(event.message).append("\n");
            }
        }
    }
}

class User {
    public final String userID;
    public final String username;
    public double balance;
    public final Locale locale;
    public boolean frozen;
    public final double minimumAllowedDeposit;
    public final double maximumAllowedDeposit;
    public final double minimumAllowedWithdrawal;
    public final double maximumAllowedWithdrawal;

    public User(String userID, String username, double balance, Locale locale, boolean frozen, double minimumAllowedDeposit, double maximumAllowedDeposit, double minimumAllowedWithdrawal, double maximumAllowedWithdrawal) {
        this.userID = userID;
        this.username = username;
        this.balance = balance;
        this.locale = locale;
        this.frozen = frozen;
        this.minimumAllowedDeposit = minimumAllowedDeposit;
        this.maximumAllowedDeposit = maximumAllowedDeposit;
        this.minimumAllowedWithdrawal = minimumAllowedWithdrawal;
        this.maximumAllowedWithdrawal = maximumAllowedWithdrawal;
    }

}

class Transaction {
    public final String transactionID;
    public final String userID;
    public final String transactionType;
    public final double amount;
    public final String method;
    public final String account; // account numbers can be IBAN accounts with letters as well

    public Transaction(String transactionID, String userID, String transactionType, double amount, String method, String account) {
        this.transactionID = transactionID;
        this.userID = userID;
        this.transactionType = transactionType;
        this.amount = amount;
        this.method = method;
        this.account = account;
    }
}

class BinMapping {
    public final String bankName;
    public final long range_from;
    public final long range_to;
    public final String type;
    public final Locale country;

    public BinMapping(String bankName, long range_from, long range_to, String type, Locale country) {
        this.bankName = bankName;
        this.range_from = range_from;
        this.range_to = range_to;
        this.type = type;
        this.country = country;
    }

}

class Event {
    public static final String STATUS_DECLINED = "DECLINED";
    public static final String STATUS_APPROVED = "APPROVED";

    public String transactionId;
    public String status;
    public String message;

    public Event(String transactionId, String status, String message) {
        this.transactionId = transactionId;
        this.status = status;
        this.message = message;
    }
}
